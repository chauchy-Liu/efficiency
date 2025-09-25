/*
 Navicat Premium Dump SQL

 Source Server         : cloud_core
 Source Server Type    : MySQL
 Source Server Version : 90100 (9.1.0)
 Source Host           : localhost:13306
 Source Schema         : cloud_core

 Target Server Type    : MySQL
 Target Server Version : 90100 (9.1.0)
 File Encoding         : 65001

 Date: 19/09/2025 17:30:03
*/

SET NAMES utf8mb4;
SET FOREIGN_KEY_CHECKS = 0;

-- ----------------------------
-- Table structure for turbineTypeToFaultCodeMap
-- ----------------------------
DROP TABLE IF EXISTS `turbineTypeToFaultCodeMap`;
CREATE TABLE `turbineTypeToFaultCodeMap` (
  `chineseName` varchar(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci DEFAULT NULL COMMENT '中文名',
  `typeName` varchar(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NOT NULL COMMENT '英文代号',
  `primeIndex` varchar(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci DEFAULT NULL COMMENT '主型号',
  `secondIndex` varchar(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci DEFAULT NULL COMMENT '二级型号',
  `thirdIndex` varchar(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci DEFAULT NULL COMMENT '三级型号',
  `fileName` varchar(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NOT NULL COMMENT '故障python文件',
  PRIMARY KEY (`fileName`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;

-- ----------------------------
-- Records of turbineTypeToFaultCodeMap
-- ----------------------------
BEGIN;
INSERT INTO `turbineTypeToFaultCodeMap` (`chineseName`, `typeName`, `primeIndex`, `secondIndex`, `thirdIndex`, `fileName`) VALUES ('远景', 'EN', '141', '3.2', NULL, 'faultcode_ENOS141__3_2.py');
INSERT INTO `turbineTypeToFaultCodeMap` (`chineseName`, `typeName`, `primeIndex`, `secondIndex`, `thirdIndex`, `fileName`) VALUES (NULL, 'EN', '141', '3.6', NULL, 'faultcode_ENOS141__3_6.py');
INSERT INTO `turbineTypeToFaultCodeMap` (`chineseName`, `typeName`, `primeIndex`, `secondIndex`, `thirdIndex`, `fileName`) VALUES (NULL, 'EN', '156', '4.8', NULL, 'faultcode_ENOS156__4_8.py');
INSERT INTO `turbineTypeToFaultCodeMap` (`chineseName`, `typeName`, `primeIndex`, `secondIndex`, `thirdIndex`, `fileName`) VALUES ('金风', 'GW', '115', '2.0', NULL, 'faultcode_GW115__2_0.py');
INSERT INTO `turbineTypeToFaultCodeMap` (`chineseName`, `typeName`, `primeIndex`, `secondIndex`, `thirdIndex`, `fileName`) VALUES (NULL, 'GW', '140', '3.57', NULL, 'faultcode_GW140__3_57.py');
INSERT INTO `turbineTypeToFaultCodeMap` (`chineseName`, `typeName`, `primeIndex`, `secondIndex`, `thirdIndex`, `fileName`) VALUES (NULL, 'GW', '151', '2.0', NULL, 'faultcode_GW151__2_0.py');
INSERT INTO `turbineTypeToFaultCodeMap` (`chineseName`, `typeName`, `primeIndex`, `secondIndex`, `thirdIndex`, `fileName`) VALUES (NULL, 'GW', '155', '4.8', NULL, 'faultcode_GW155__4_8.py');
INSERT INTO `turbineTypeToFaultCodeMap` (`chineseName`, `typeName`, `primeIndex`, `secondIndex`, `thirdIndex`, `fileName`) VALUES (NULL, 'GWH', '171', '5000', NULL, 'faultcode_GWH171__5000.py');
INSERT INTO `turbineTypeToFaultCodeMap` (`chineseName`, `typeName`, `primeIndex`, `secondIndex`, `thirdIndex`, `fileName`) VALUES (NULL, 'GWH', '191', '5000', NULL, 'faultcode_GWH191__5000');
INSERT INTO `turbineTypeToFaultCodeMap` (`chineseName`, `typeName`, `primeIndex`, `secondIndex`, `thirdIndex`, `fileName`) VALUES (NULL, 'GWH', '191', '5.0', NULL, 'faultcode_GWH191__5_0.py');
INSERT INTO `turbineTypeToFaultCodeMap` (`chineseName`, `typeName`, `primeIndex`, `secondIndex`, `thirdIndex`, `fileName`) VALUES (NULL, 'GWH', '191', '6.7', NULL, 'faultcode_GWH191__6_7.py');
INSERT INTO `turbineTypeToFaultCodeMap` (`chineseName`, `typeName`, `primeIndex`, `secondIndex`, `thirdIndex`, `fileName`) VALUES ('明阳', 'MY', '2.0', '104', NULL, 'faultcode_MY2_0__104.py');
INSERT INTO `turbineTypeToFaultCodeMap` (`chineseName`, `typeName`, `primeIndex`, `secondIndex`, `thirdIndex`, `fileName`) VALUES (NULL, 'MY', '2.0', '110', NULL, 'faultcode_MY2_0__110.py');
INSERT INTO `turbineTypeToFaultCodeMap` (`chineseName`, `typeName`, `primeIndex`, `secondIndex`, `thirdIndex`, `fileName`) VALUES (NULL, 'MY', '3.2', '', NULL, 'faultcode_MY3_2.py');
INSERT INTO `turbineTypeToFaultCodeMap` (`chineseName`, `typeName`, `primeIndex`, `secondIndex`, `thirdIndex`, `fileName`) VALUES ('', 'MY', '3.6', NULL, NULL, 'faultcode_MY3_6.py');
INSERT INTO `turbineTypeToFaultCodeMap` (`chineseName`, `typeName`, `primeIndex`, `secondIndex`, `thirdIndex`, `fileName`) VALUES (NULL, 'MY', '4.0', NULL, NULL, 'faultcode_MY4_0.py');
INSERT INTO `turbineTypeToFaultCodeMap` (`chineseName`, `typeName`, `primeIndex`, `secondIndex`, `thirdIndex`, `fileName`) VALUES (NULL, 'MY', '5.0', NULL, NULL, 'faultcode_MY5_0.py');
INSERT INTO `turbineTypeToFaultCodeMap` (`chineseName`, `typeName`, `primeIndex`, `secondIndex`, `thirdIndex`, `fileName`) VALUES (NULL, 'MY', '6.25', NULL, NULL, 'faultcode_MY6_25.py');
INSERT INTO `turbineTypeToFaultCodeMap` (`chineseName`, `typeName`, `primeIndex`, `secondIndex`, `thirdIndex`, `fileName`) VALUES (NULL, 'MY', '6.7', NULL, NULL, 'faultcode_MY6_7.py');
INSERT INTO `turbineTypeToFaultCodeMap` (`chineseName`, `typeName`, `primeIndex`, `secondIndex`, `thirdIndex`, `fileName`) VALUES ('三一', 'SE', '115', '2.0', NULL, 'faultcode_SE115__2_0.py');
INSERT INTO `turbineTypeToFaultCodeMap` (`chineseName`, `typeName`, `primeIndex`, `secondIndex`, `thirdIndex`, `fileName`) VALUES (NULL, 'SE', '15645', NULL, NULL, 'faultcode_SE15645.py');
INSERT INTO `turbineTypeToFaultCodeMap` (`chineseName`, `typeName`, `primeIndex`, `secondIndex`, `thirdIndex`, `fileName`) VALUES (NULL, 'SI', '17267', NULL, NULL, 'faultcode_SI17267.py');
INSERT INTO `turbineTypeToFaultCodeMap` (`chineseName`, `typeName`, `primeIndex`, `secondIndex`, `thirdIndex`, `fileName`) VALUES ('湘电/哈电', 'XE', '116', '2.0', NULL, 'faultcode_XE116__2_0.py');
COMMIT;

SET FOREIGN_KEY_CHECKS = 1;
