-- Вторая база для orders-svc. Один Postgres, две изолированные базы:
-- cross-database JOIN в Postgres невозможен, что и требуется.
CREATE DATABASE orders OWNER app;
-- Третья база: пользователи gateway (auth). Три базы, три владельца, ноль общих таблиц.
CREATE DATABASE auth OWNER app;
