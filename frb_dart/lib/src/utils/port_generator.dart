import 'dart:math';

import 'package:meta/meta.dart';

/// On web these names become origin-shared BroadcastChannel names, so they must be unique per app instance (tab) or tabs receive each other's messages.
final String _instanceId =
    '${DateTime.now().microsecondsSinceEpoch.toRadixString(36)}'
    '${Random().nextInt(1 << 31).toRadixString(36)}';

/// {@macro flutter_rust_bridge.internal}
@internal
class ExecuteStreamPortGenerator {
  static final _streamSinkNameIndex = <String, int>{};

  /// {@macro flutter_rust_bridge.internal}
  static String create(String funcName) {
    final nextIndex = _streamSinkNameIndex
        .update(funcName, (value) => value + 1, ifAbsent: () => 0);
    return '__frb_streamsink_${_instanceId}_${funcName}_$nextIndex';
  }
}

/// {@macro flutter_rust_bridge.internal}
@internal
class BaseLazyPortIdGenerator {
  static int _nextPort = 0;

  /// {@macro flutter_rust_bridge.internal}
  static String create() => '__frb_lazy_port_${_instanceId}_${_nextPort++}';
}
