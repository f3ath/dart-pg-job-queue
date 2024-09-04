import 'dart:io';

import 'package:postgres/postgres.dart';

Future<Connection> createConnection() {
  final env = Platform.environment;
  return Connection.open(
      Endpoint(
        host: env['PG_HOST'] ?? 'localhost',
        port: int.fromEnvironment('PG_PORT', defaultValue: 5432),
        database: env['PG_DATABASE'] ?? 'postgres',
        username: env['PG_USER'] ?? 'postgres',
        password: env['PG_PASSWORD'] ?? 'postgres',
      ),
      settings: ConnectionSettings(sslMode: SslMode.disable));
}
