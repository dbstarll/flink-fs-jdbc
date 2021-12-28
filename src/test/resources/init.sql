CREATE TABLE `test` (
  `id` bigint NOT NULL AUTO_INCREMENT,
  `parent` bigint NOT NULL DEFAULT '0',
  `name` varchar(255) CHARACTER SET utf8 COLLATE utf8_bin NOT NULL,
  `path` varchar(1024) CHARACTER SET utf8 COLLATE utf8_bin NOT NULL,
  `file` tinyint NOT NULL DEFAULT '0',
  `len` bigint NOT NULL DEFAULT '0',
  `data` longblob,
  `created` bigint NOT NULL,
  `modified` bigint NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `path_UNIQUE` (`path`),
  UNIQUE KEY `file_UNIQUE` (`parent`,`name`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin;
