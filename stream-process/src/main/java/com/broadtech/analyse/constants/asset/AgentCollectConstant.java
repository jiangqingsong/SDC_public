package com.broadtech.analyse.constants.asset;

/**
 * @author jiangqingsong
 * @description Agent代理采集常量
 * @date 2020-06-09 13:55
 */
public class AgentCollectConstant {
    //agent json字段信息
    public static final String RESOURCE_NAME = "ResourceName";
    public static final String TASK_ID = "TaskID";
    public static final String ASSET_ID = "AssetID";
    public static final String SCAN_TIME = "ScanTime";
    public static final String RESOURCE_INFO = "ResourceInfo";
    public static final String DEVICE_NAME = "DeviceName";
    public static final String DEVICE_TYPE = "DeviceType";
    public static final String DEVICE_IP_ADDRESS = "DeviceIPAddress";
    public static final String OS_INFO = "OSInfo";
    public static final String PATCH_PROPERTIES = "PatchProperties";
    public static final String KERNEL_VERSION = "KernelVersion";
    public static final String OPEN_SERVICE_OF_PORT = "OpenServiceOfPort";
    public static final String PROGRAM_INFO = "ProgramInfo";
    public static final String START_UP = "start_up";
    public static final String MESSAGE_ORIENTED_MIDDLEWARE = "MessageOrientedMiddleware";
    public static final String DATA_BASE_INFO = "DataBaseInfo";

    //资产发现表字段
    //表名
    public static final String TAB_NAME_VULNERABILITY = "asset_agent_with_vulnerability";
    public static final String TAB_NAME_LABEL = "asset_agent_with_label";
    //JDBC
    public static final String JDBC_URL = "jdbcUrl";
    public static final String JDBC_USER = "username";
    public static final String JDBC_PWD = "password";
    public static final String JDBC_DRIVER = "driverClass";
}
