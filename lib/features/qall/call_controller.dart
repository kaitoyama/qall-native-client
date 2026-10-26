import 'dart:async';
import 'dart:convert';
import 'dart:io';

import 'package:flutter/foundation.dart';
import 'package:flutter_riverpod/flutter_riverpod.dart';
import 'package:flutter_webrtc/flutter_webrtc.dart' show Helper;
import 'package:livekit_client/livekit_client.dart';
// BroadcastManager (iOS の画面共有の開始/停止) は公開APIから export されていない
// ignore: implementation_imports
import 'package:livekit_client/src/managers/broadcast_manager.dart';
import 'package:permission_handler/permission_handler.dart';
import 'package:qall_audio/qall_audio.dart';

import '../../core/models.dart';
import '../../core/providers.dart';
import '../../core/speech_format.dart';
import '../../core/traq_api.dart';
import '../settings/call_settings.dart';
import 'call_background.dart';
import 'call_channel_feed.dart';

enum CallStatus { idle, connecting, connected, reconnecting }

/// 通話の参加者 (画面表示用)。LiveKit の Participant から必要な情報だけ取り出したもの。
class CallParticipant {
  const CallParticipant({
    required this.userId,
    required this.isLocal,
    this.isSpeaking = false,
    this.isMuted = false,
    this.isScreenSharing = false,
  });

  /// traQ のユーザーID
  final String userId;
  final bool isLocal;
  final bool isSpeaking;
  final bool isMuted;
  final bool isScreenSharing;
}

/// 画面に表示する映像トラック (画面共有 or カメラ)。
class CallVideo {
  const CallVideo({
    required this.userId,
    required this.track,
    required this.isScreenShare,
  });

  /// 映像を送っている参加者の traQ ユーザーID
  final String userId;
  final VideoTrack track;
  final bool isScreenShare;

  String get id => track.sid ?? track.mediaStreamTrack.id ?? '';
}

/// 他の参加者から届いたスタンプリアクション。
class StampReaction {
  const StampReaction({required this.stampId, required this.userId});

  final String stampId;
  final String? userId;
}

class CallException implements Exception {
  const CallException(this.message);

  final String message;

  @override
  String toString() => message;
}

/// LiveKit の接続先が暗号化された接続か。
/// traQ の `GET /qall/endpoints` は `https://` を返し、LiveKit SDK はそれを `wss://` に変換して接続する。
@visibleForTesting
bool isSecureCallEndpoint(String endpoint) {
  final scheme = Uri.tryParse(endpoint)?.scheme;
  return scheme == 'https' || scheme == 'wss';
}

final qallCallProvider = Provider<QallCallController>((ref) {
  final session = ref.watch(sessionProvider);
  final controller = QallCallController(
    api: session.api,
    myUserId: session.me.id,
    channelName: (id) =>
        ref.read(channelTreeProvider).value?.displayName(id) ?? 'Qall',
    userName: (id) => ref.read(usersProvider).value?[id]?.displayName ?? '',
    settings: () => ref.read(callSettingsProvider),
  );
  ref.listen(
    callSettingsProvider,
    (previous, next) => controller.applySettings(previous, next),
  );
  ref.onDispose(controller.dispose);
  return controller;
});

/// Qall (LiveKit) の通話を1つ管理する。
///
/// traQ Web版 (traQ_S-UI) との互換性のために以下を守る:
/// - ルーム名はチャンネルID。トークンは `GET /qall/token?roomId=` で取得する
/// - マイクのトラックを必ず publish する (Web版の参加者一覧はマイクトラックの有無で判定している)
/// - スタンプリアクションは `{"type":"stamp","message":stampId}` の data メッセージ
class QallCallController extends ChangeNotifier {
  QallCallController({
    required this.api,
    required this.channelName,
    this.myUserId = '',
    String Function(String userId)? userName,
    CallSettings Function()? settings,
  }) : userName = userName ?? ((_) => ''),
       settings = settings ?? (() => const CallSettings()) {
    _background = CallBackgroundService(
      onNotificationAction: _onNotificationAction,
    );
    if (Platform.isIOS) {
      // 画面収録の開始/停止は Darwin 通知で届くが、通知名は端末全体で共有されていて
      // 他のアプリも送れる。ユーザーが共有ボタンを押したときだけ publish する
      BroadcastManager().shouldPublishTrack = false;
      BroadcastManager().addListener(_onBroadcastStateChanged);
    }
  }

  final TraqApi api;
  final String myUserId;
  final String Function(String channelId) channelName;
  final String Function(String userId) userName;
  final CallSettings Function() settings;
  late final CallBackgroundService _background;
  CallChannelFeed? _feed;
  StreamSubscription<Message>? _feedSubscription;
  final _channelMessages = StreamController<Message>.broadcast();
  bool _isWebinar = false;

  Room? _room;
  EventsListener<RoomEvent>? _listener;
  String? _channelId;
  CallStatus _status = CallStatus.idle;
  bool _speakerOn = true;
  bool _disposed = false;
  int _attempt = 0;
  final _reactions = StreamController<StampReaction>.broadcast();
  Timer? _broadcastRequestTimeout;

  AudioCaptureOptions get _micOptions {
    final s = settings();
    final standard = s.noiseReduction == NoiseReduction.standard;
    return AudioCaptureOptions(
      // ミュート中もマイクのキャプチャを止めない。
      // iOS で音声セッションを動かし続け、バックグラウンドで停止されないようにするため。
      stopAudioCaptureOnMute: false,
      // RNNoise を使うときは WebRTC 標準のノイズ抑制と二重にかけない
      noiseSuppression: standard,
      voiceIsolation: standard,
      typingNoiseDetection: standard,
      echoCancellation: s.echoCancellation,
      autoGainControl: s.autoGainControl,
    );
  }

  RoomOptions get _roomOptions => RoomOptions(
    adaptiveStream: true,
    dynacast: true,
    defaultAudioCaptureOptions: _micOptions,
    defaultAudioPublishOptions: const AudioPublishOptions(dtx: false),
    defaultCameraCaptureOptions: const CameraCaptureOptions(
      params: VideoParametersPresets.h720_169,
    ),
    defaultScreenShareCaptureOptions: const ScreenShareCaptureOptions(
      // iOS: 他のアプリの画面も共有できるよう Broadcast Upload Extension を使う
      useiOSBroadcastExtension: true,
    ),
  );

  String? get channelId => _channelId;
  CallStatus get status => _status;
  bool get inCall => status != CallStatus.idle;
  Room? get room => _room;
  Stream<StampReaction> get reactions => _reactions.stream;

  /// 通話中のチャンネルに投稿されたメッセージ (弾幕用)
  Stream<Message> get channelMessages => _channelMessages.stream;

  /// ウェビナーとして開始/参加したルームか
  bool get isWebinar => _isWebinar;

  LocalParticipant? get _local => _room?.localParticipant;
  bool get canPublish => _local?.permissions.canPublish ?? false;
  bool get isMicEnabled => _local?.isMicrophoneEnabled() ?? false;
  bool get isScreenSharing => _local?.isScreenShareEnabled() ?? false;
  bool get isCameraEnabled => _local?.isCameraEnabled() ?? false;
  bool get isSpeakerOn => _speakerOn;
  CameraPosition _cameraPosition = CameraPosition.front;
  CameraPosition get cameraPosition => _cameraPosition;

  /// 自分を先頭にした LiveKit の参加者 (サウンドボードの再生用参加者は除く)。
  Iterable<(String, Participant)> get _liveKitParticipants sync* {
    final room = _room;
    if (room == null) return;
    for (final p in <Participant>[
      ?room.localParticipant,
      ...room.remoteParticipants.values,
    ]) {
      if (userIdFromIdentity(p.identity) case final userId?) yield (userId, p);
    }
  }

  /// 自分を先頭にした参加者一覧。
  List<CallParticipant> get participants => [
    for (final (userId, p) in _liveKitParticipants)
      CallParticipant(
        userId: userId,
        isLocal: p is LocalParticipant,
        isSpeaking: p.isSpeaking,
        isMuted: !p.isMicrophoneEnabled(),
        isScreenSharing: p.isScreenShareEnabled(),
      ),
  ];

  /// 表示可能な映像。画面共有を先に並べる。
  List<CallVideo> get videos {
    final result = <CallVideo>[];
    for (final (userId, participant) in _liveKitParticipants) {
      for (final pub in participant.videoTrackPublications) {
        final track = pub.track;
        if (track is! VideoTrack || pub.muted) continue;
        result.add(
          CallVideo(
            userId: userId,
            track: track,
            isScreenShare: pub.source == TrackSource.screenShareVideo,
          ),
        );
      }
    }
    result.sort(
      (a, b) => (a.isScreenShare ? 0 : 1) - (b.isScreenShare ? 0 : 1),
    );
    return result;
  }

  // ---- 参加 / 退出 ----

  /// [webinar] が true なら、ルームがまだなければウェビナーとして作る
  /// (後から入った人は発言権限なしで参加する)。
  Future<void> join(String channelId, {bool webinar = false}) async {
    if (_channelId == channelId && inCall) return;
    if (inCall) await leave();

    // 接続中に leave() されたら、この join は以降何もしない
    final attempt = ++_attempt;
    bool cancelled() => attempt != _attempt;

    _channelId = channelId;
    _isWebinar = webinar;
    _status = CallStatus.connecting;
    _notify();

    try {
      final micGranted = await _requestMicrophonePermission();
      final endpoint = await api.getQallEndpoint();
      // LiveKit のトークンを平文で送らないよう、暗号化された接続先だけを使う
      if (!isSecureCallEndpoint(endpoint)) {
        throw const CallException('通話サーバーの接続先が安全ではありません (https / wss ではありません)');
      }
      final token = await api.getQallToken(channelId, isWebinar: webinar);
      if (cancelled()) return;

      // Android: 接続前に前面サービスを開始し、以降スリープしても切れないようにする
      await _background.start(
        channelName: channelName(channelId),
        useMicrophone: micGranted,
      );
      if (cancelled()) {
        await _background.stop();
        return;
      }

      final room = Room(roomOptions: _roomOptions);
      _room = room;
      _listener = room.createListener()..listen(_onRoomEvent);
      await room.connect(endpoint, token);
      if (cancelled()) return;
      _isWebinar = _isWebinar || _roomIsWebinar(room.metadata);

      if (canPublish && micGranted) {
        await room.localParticipant?.setMicrophoneEnabled(
          true,
          audioCaptureOptions: _micOptions,
        );
      }
      await AudioManager.instance.setSpeakerOutputPreferred(_speakerOn);
      if (cancelled()) return;
      _status = CallStatus.connected;
      _startFeed(channelId);
      await _applyNoiseReduction(settings());
      _applyVolumes();
      _playEffect('qall_start');
      await _updateNotification();
    } catch (e) {
      if (cancelled()) return;
      debugPrint('Qall join failed: $e');
      await _teardown();
      throw CallException(
        '通話に参加できませんでした: ${switch (e) {
          TraqApiException(:final message) || CallException(:final message) => message,
          _ => e,
        }}',
      );
    } finally {
      _notify();
    }
  }

  Future<void> leave() async {
    if (!inCall) return;
    final wasConnected = status != CallStatus.connecting;
    await _teardown();
    if (wasConnected) _playEffect('qall_end');
    _notify();
  }

  Future<void> _teardown() async {
    _attempt++;
    final room = _room;
    final listener = _listener;
    _room = null;
    _listener = null;
    _channelId = null;
    _isWebinar = false;
    _status = CallStatus.idle;

    await _stopFeed();
    unawaited(QallAudio.stopSpeaking().catchError((Object _) {}));
    if (Platform.isIOS && BroadcastManager().isBroadcasting) {
      await BroadcastManager().requestStop();
    }
    await listener?.dispose();
    try {
      await room?.disconnect();
    } catch (e) {
      debugPrint('Qall disconnect failed: $e');
    }
    await room?.dispose();
    await _background.stop();
  }

  /// traQ はルームのメタデータに `{"status": ..., "isWebinar": ...}` を入れている。
  static bool _roomIsWebinar(String? metadata) {
    if (metadata == null || metadata.isEmpty) return false;
    try {
      final json = jsonDecode(metadata);
      return json is Map && json['isWebinar'] == true;
    } on FormatException {
      return false;
    }
  }

  // ---- 操作 ----

  Future<void> setMicrophoneEnabled(bool enabled) async {
    final local = _local;
    if (local == null) return;
    if (!canPublish) throw const CallException('このルームでは発言権限がありません');
    if (enabled && !await _requestMicrophonePermission()) {
      throw const CallException('マイクの使用が許可されていません');
    }
    await local.setMicrophoneEnabled(enabled, audioCaptureOptions: _micOptions);
    await _updateNotification();
    _notify();
  }

  Future<void> toggleMicrophone() => setMicrophoneEnabled(!isMicEnabled);

  Future<void> setSpeakerOn(bool on) async {
    _speakerOn = on;
    await AudioManager.instance.setSpeakerOutputPreferred(on);
    _notify();
  }

  /// 画面共有を開始/停止する。
  ///
  /// - Android: MediaProjection の許可ダイアログを出し、許可されたら共有する
  /// - iOS: 画面収録 (ブロードキャスト) の開始ピッカーを出す。ユーザーが開始すると
  ///   LiveKit が自動で publish する
  Future<void> setScreenShareEnabled(bool enabled) async {
    final local = _local;
    final channelId = _channelId;
    if (local == null || channelId == null) return;
    if (!canPublish) throw const CallException('このルームでは発言権限がありません');

    if (!enabled) {
      if (Platform.isIOS && BroadcastManager().isBroadcasting) {
        // ブロードキャストが終わると LiveKit がトラックを unpublish する
        await BroadcastManager().requestStop();
      } else {
        await local.setScreenShareEnabled(false);
      }
      await _background.setScreenSharing(false);
      await _updateNotification();
      _notify();
      return;
    }

    if (Platform.isIOS) _allowBroadcastPublish(true);
    if (Platform.isAndroid) {
      final granted = await Helper.requestCapturePermission();
      if (!granted) return;
      // Android 14+: 許可を得た後に mediaProjection タイプのサービスを開始する必要がある
      await _background.setScreenSharing(true);
    }
    try {
      await local.setScreenShareEnabled(true);
    } catch (e) {
      await _background.setScreenSharing(false);
      throw CallException('画面共有を開始できませんでした: $e');
    }
    await _updateNotification();
    _notify();
  }

  Future<void> toggleScreenShare() => setScreenShareEnabled(!isScreenSharing);

  /// iOS: 画面収録が始まったら publish してよいか。ユーザーが共有ボタンを押してから
  /// ピッカーで開始するまでの間だけ許可する (開始しなかった場合は時間切れで戻す)。
  void _allowBroadcastPublish(bool allow) {
    _broadcastRequestTimeout?.cancel();
    _broadcastRequestTimeout = null;
    BroadcastManager().shouldPublishTrack = allow;
    if (allow) {
      _broadcastRequestTimeout = Timer(
        const Duration(minutes: 3),
        () => _allowBroadcastPublish(false),
      );
    }
  }

  void _onBroadcastStateChanged() {
    if (BroadcastManager().isBroadcasting) {
      // 開始の通知は届いたので、時間切れのタイマーだけ止める (停止までは publish を保つ)
      _broadcastRequestTimeout?.cancel();
      _broadcastRequestTimeout = null;
    } else {
      _allowBroadcastPublish(false);
    }
    _notify();
  }

  /// カメラ映像の送信を開始/停止する。
  Future<void> setCameraEnabled(bool enabled) async {
    final local = _local;
    if (local == null) return;
    if (!canPublish) throw const CallException('このルームでは発言権限がありません');
    if (enabled) {
      final status = await Permission.camera.request();
      if (!status.isGranted) {
        throw const CallException('カメラの使用が許可されていません');
      }
      // Android 14+: バックグラウンドでもカメラを使い続けるには camera タイプのサービスが必要
      await _background.setCamera(true);
    }
    try {
      await local.setCameraEnabled(
        enabled,
        cameraCaptureOptions: CameraCaptureOptions(
          cameraPosition: _cameraPosition,
          params: VideoParametersPresets.h720_169,
        ),
      );
    } catch (e) {
      if (enabled) await _background.setCamera(false);
      throw CallException('カメラを${enabled ? '開始' : '停止'}できませんでした: $e');
    }
    if (!enabled) await _background.setCamera(false);
    _notify();
  }

  Future<void> toggleCamera() => setCameraEnabled(!isCameraEnabled);

  /// 前面/背面カメラを切り替える。
  Future<void> switchCamera() async {
    final next = _cameraPosition == CameraPosition.front
        ? CameraPosition.back
        : CameraPosition.front;
    final track = _local
        ?.getTrackPublicationBySource(TrackSource.camera)
        ?.track;
    if (track is LocalVideoTrack) {
      await track.setCameraPosition(next);
    }
    _cameraPosition = next;
    _notify();
  }

  /// スタンプでリアクションする (Web版と同じ data メッセージ)。
  Future<void> sendStamp(String stampId) async {
    final local = _local;
    if (local == null) return;
    final payload = utf8.encode(
      jsonEncode({'type': 'stamp', 'message': stampId}),
    );
    await local.publishData(payload, reliable: true);
    _reactions.add(StampReaction(stampId: stampId, userId: myUserId));
  }

  /// サウンドボードの音をルームに流す (traQ サーバーが LiveKit に配信する)。
  Future<void> playSoundboard(String soundId) async {
    final channelId = _channelId;
    if (channelId == null) return;
    try {
      await api.playSoundboard(soundId: soundId, roomId: channelId);
    } on TraqApiException catch (e) {
      throw CallException('再生できませんでした: ${e.message}');
    }
  }

  /// Android: 音声の出力先の候補 (スピーカー / 受話口 / Bluetooth など)。
  Future<List<MediaDevice>> audioOutputs() => Hardware.instance.audioOutputs();

  Future<void> selectAudioOutput(MediaDevice device) async {
    await Hardware.instance.selectAudioOutput(device);
    _notify();
  }

  /// 設定画面での変更を通話中に反映する。
  void applySettings(CallSettings? previous, CallSettings next) {
    if (!inCall) return;
    if (previous?.noiseReduction != next.noiseReduction ||
        previous?.echoCancellation != next.echoCancellation ||
        previous?.autoGainControl != next.autoGainControl) {
      unawaited(_applyNoiseReduction(next));
    }
    if (previous?.masterVolume != next.masterVolume ||
        previous?.userVolumes != next.userVolumes) {
      _applyVolumes();
    }
    if (previous?.ttsEnabled == true && !next.ttsEnabled) {
      unawaited(QallAudio.stopSpeaking());
    }
  }

  // ---- 内部 ----

  Future<void> _applyNoiseReduction(CallSettings s) async {
    final track = _local
        ?.getTrackPublicationBySource(TrackSource.microphone)
        ?.track;
    if (track is LocalAudioTrack) {
      try {
        // 通話中にマイクのトラックを作り直さず切り替えられる唯一の API (LiveKit では experimental)
        // ignore: experimental_member_use
        await track.setAudioProcessingOptions(
          // ignore: experimental_member_use
          AudioProcessingOptions(
            noiseSuppression: s.noiseReduction == NoiseReduction.standard,
            echoCancellation: s.echoCancellation,
            autoGainControl: s.autoGainControl,
            highPassFilter: s.noiseReduction == NoiseReduction.standard,
          ),
        );
      } catch (e) {
        debugPrint('Failed to update audio processing: $e');
      }
    }
    try {
      final ok = await QallAudio.setNoiseSuppression(
        s.noiseReduction == NoiseReduction.rnnoise,
      );
      if (!ok) debugPrint('RNNoise could not be enabled');
    } catch (e) {
      debugPrint('RNNoise failed: $e');
    }
  }

  /// マスターボリュームと参加者ごとの音量を、受信している音声トラックに反映する。
  void _applyVolumes() {
    final room = _room;
    if (room == null) return;
    final s = settings();
    for (final participant in room.remoteParticipants.values) {
      final userId = userIdFromIdentity(participant.identity);
      // サウンドボードなど traQ ユーザーでない参加者はマスターボリュームのみ
      final volume = userId == null
          ? s.masterVolume
          : s.effectiveVolume(userId);
      for (final pub in participant.audioTrackPublications) {
        final track = pub.track;
        if (track == null) continue;
        unawaited(
          Helper.setVolume(
            volume,
            track.mediaStreamTrack,
          ).catchError((Object e) => debugPrint('Failed to set volume: $e')),
        );
      }
    }
  }

  void _playEffect(String name) {
    final s = settings();
    if (!s.soundEffects) return;
    // Web版と同じく、効果音はマスターボリュームの半分の大きさ
    final volume = (s.masterVolume * 0.5).clamp(0.0, 1.0);
    unawaited(
      QallAudio.playSound(
        'assets/se/$name.mp3',
        volume: volume,
      ).catchError((Object e) => debugPrint('Failed to play $name: $e')),
    );
  }

  void _startFeed(String channelId) {
    final feed = CallChannelFeed(api, channelId);
    _feed = feed;
    _feedSubscription = feed.messages.listen((message) {
      if (!_channelMessages.isClosed) _channelMessages.add(message);
      _speak(message);
    });
  }

  Future<void> _stopFeed() async {
    final feed = _feed;
    _feed = null;
    await _feedSubscription?.cancel();
    _feedSubscription = null;
    await feed?.dispose();
  }

  /// 通話中のチャンネルに届いたメッセージを読み上げる (自分の投稿は除く)。
  void _speak(Message message) {
    final s = settings();
    if (!s.ttsEnabled || message.userId == myUserId) return;
    final text = formatForSpeech(
      userDisplayName: userName(message.userId),
      content: message.content,
      origin: api.origin,
    );
    unawaited(
      QallAudio.speak(
        text,
        voice: s.ttsVoice,
        pitch: s.ttsPitch,
        rate: s.ttsRate,
        volume: s.ttsVolume,
      ).catchError((Object e) => debugPrint('TTS failed: $e')),
    );
  }

  Future<bool> _requestMicrophonePermission() async {
    final statuses = await [
      Permission.microphone,
      // Android 12+: Bluetooth ヘッドセットに音声を流すのに必要 (拒否されても通話はできる)
      if (Platform.isAndroid) Permission.bluetoothConnect,
    ].request();
    final mic = statuses[Permission.microphone];
    return mic != null && (mic.isGranted || mic.isLimited);
  }

  void _onRoomEvent(RoomEvent event) {
    switch (event) {
      case RoomReconnectingEvent() || RoomAttemptReconnectEvent():
        _status = CallStatus.reconnecting;
      case RoomReconnectedEvent():
        _status = CallStatus.connected;
      case RoomDisconnectedEvent():
        // 自分で leave() した場合は _listener が先に破棄されるのでここには来ない
        unawaited(
          _teardown().whenComplete(() {
            _playEffect('qall_end');
            _notify();
          }),
        );
        return;
      case DataReceivedEvent():
        _handleData(event);
      case LocalTrackUnpublishedEvent(:final publication)
          when publication.source == TrackSource.screenShareVideo:
        // iOS のコントロールセンター等から共有を止めた場合
        unawaited(_background.setScreenSharing(false));
        unawaited(_updateNotification());
      case TrackSubscribedEvent(:final track) when track is AudioTrack:
        _applyVolumes();
      case ParticipantConnectedEvent(:final participant)
          when userIdFromIdentity(participant.identity) != null:
        _playEffect('qall_joined');
      case ParticipantDisconnectedEvent(:final participant)
          when userIdFromIdentity(participant.identity) != null:
        _playEffect('qall_left');
      case RoomMetadataChangedEvent(:final metadata):
        _isWebinar = _isWebinar || _roomIsWebinar(metadata);
      default:
        break;
    }
    _notify();
  }

  void _handleData(DataReceivedEvent event) {
    try {
      final json = jsonDecode(utf8.decode(event.data));
      // 同じ通話の参加者なら誰でも送れるデータなので、スタンプIDの形を確かめる
      if (json case {'type': 'stamp', 'message': final String stampId}
          when isUuid(stampId)) {
        _reactions.add(
          StampReaction(
            stampId: stampId,
            userId: userIdFromIdentity(event.participant?.identity ?? ''),
          ),
        );
      }
    } on FormatException {
      // 知らない形式のデータは無視
    }
  }

  void _onNotificationAction(String action) {
    switch (action) {
      case CallNotificationAction.toggleMute:
        unawaited(toggleMicrophone().catchError((Object _) {}));
      case CallNotificationAction.hangUp:
        unawaited(leave());
    }
  }

  Future<void> _updateNotification() async {
    final channelId = _channelId;
    if (channelId == null) return;
    await _background.update(
      channelName: channelName(channelId),
      muted: !isMicEnabled,
      screenSharing: isScreenSharing,
    );
  }

  void _notify() {
    if (!_disposed) notifyListeners();
  }

  @override
  void dispose() {
    _disposed = true;
    if (Platform.isIOS) {
      BroadcastManager().removeListener(_onBroadcastStateChanged);
      _allowBroadcastPublish(false);
    }
    unawaited(_teardown());
    _reactions.close();
    _channelMessages.close();
    super.dispose();
  }
}
