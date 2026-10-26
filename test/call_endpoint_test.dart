import 'package:flutter_test/flutter_test.dart';
import 'package:qall/features/qall/call_controller.dart';

void main() {
  test('https and wss endpoints are accepted', () {
    expect(isSecureCallEndpoint('https://livekit.qall.trap.jp'), isTrue);
    expect(isSecureCallEndpoint('wss://livekit.qall.trap.jp'), isTrue);
    expect(isSecureCallEndpoint('HTTPS://livekit.qall.trap.jp'), isTrue);
  });

  test('plaintext or malformed endpoints are rejected', () {
    expect(isSecureCallEndpoint('http://livekit.qall.trap.jp'), isFalse);
    expect(isSecureCallEndpoint('ws://livekit.qall.trap.jp'), isFalse);
    expect(isSecureCallEndpoint('livekit.qall.trap.jp'), isFalse);
    expect(isSecureCallEndpoint(''), isFalse);
    expect(isSecureCallEndpoint('https://[::1'), isFalse);
  });
}
