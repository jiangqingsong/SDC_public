package com.broadtech.analyse.task.offline.ss;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.jdbc.JDBCOutputFormat;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.MapOperator;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.types.Row;

/**
 * @author leo.J
 * @description
 * @date 2020-08-11 16:47
 */
public class LoadIps {
    public static void main(String[] args) throws Exception {

        String driverClass = "com.mysql.cj.jdbc.Driver";
        String dbUrl = "jdbc:mysql://192.168.5.93:3306/sdc?useUnicode=true&characterEncoding=utf8&serverTimezone=GMT";
        String userNmae = "root";
        String passWord = "broadtech";
        String sql = "insert into ss_ids_log (index1 ,type1,id,score,source_lid,source_imergecount,source_ceventname," +
                "source_ceventdigest,source_ceventtype,source_icollecttype,source_ieventlevel,source_iprotocol,source_iappprotocol," +
                "source_csrcname,source_csrcmac,source_csrcip,source_csrctip,source_isrcport,source_isrctport,source_cdstname," +
                "source_cdstmac,source_cdstip,source_cdsttip,source_idstport,source_idsttport,source_cusername,source_cprogram," +
                "source_coperation,source_cobject,source_iresult,source_ireponse,source_cdevname,source_cdevtype,source_cdevip," +
                "source_loccurtime,source_lrecepttime,source_ccollectorip,source_lsend,source_lreceive,source_lduration,source_dmonitorvalue," +
                "source_corilevel,source_coritype,source_crequestmsg,source_ceventmsg,source_eventfields_istandby2,source_eventfields_istandby3," +
                "source_eventfields_istandby1,source_eventfields_cstandby11,source_eventfields_cstandby12,source_eventfields_cstandby10," +
                "source_eventfields_lstandby4,source_eventfields_lstandby3,source_eventfields_lstandby2,source_eventfields_lstandby1," +
                "source_eventfields_lstandby6,source_eventfields_lstandby5,source_eventfields_istandby6,source_eventfields_istandby4," +
                "source_eventfields_istandby5,source_eventfields_dstandby1,source_eventfields_dstandby2,source_eventfields_cstandby1," +
                "source_eventfields_cstandby6,source_eventfields_cstandby7,source_eventfields_cstandby8,source_eventfields_cstandby9," +
                "source_eventfields_cstandby2,source_eventfields_cstandby3,source_eventfields_cstandby4,source_eventfields_cstandby5) " +
                "values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();



        String filePath = "D:\\SDC\\data\\态势感知\\日志数据\\ids.csv";
        DataSource<SecurityLogPojo> securityLogPojoDataSource = env.readCsvFile(filePath).ignoreFirstLine().pojoType(SecurityLogPojo.class, "index1", "type1", "id", "score", "source_lid",
                "source_imergecount", "source_ceventname", "source_ceventdigest", "source_ceventtype", "source_icollecttype", "source_ieventlevel",
                "source_iprotocol", "source_iappprotocol", "source_csrcname", "source_csrcmac", "source_csrcip", "source_csrctip", "source_isrcport",
                "source_isrctport", "source_cdstname", "source_cdstmac", "source_cdstip", "source_cdsttip", "source_idstport", "source_idsttport",
                "source_cusername", "source_cprogram", "source_coperation", "source_cobject", "source_iresult", "source_ireponse", "source_cdevname",
                "source_cdevtype", "source_cdevip", "source_loccurtime", "source_lrecepttime", "source_ccollectorip", "source_lsend", "source_lreceive",
                "source_lduration", "source_dmonitorvalue", "source_corilevel", "source_coritype", "source_crequestmsg", "source_ceventmsg",
                "source_eventfields_istandby2", "source_eventfields_istandby3", "source_eventfields_istandby1", "source_eventfields_cstandby11",
                "source_eventfields_cstandby12", "source_eventfields_cstandby10", "source_eventfields_lstandby4", "source_eventfields_lstandby3",
                "source_eventfields_lstandby2", "source_eventfields_lstandby1", "source_eventfields_lstandby6", "source_eventfields_lstandby5",
                "source_eventfields_istandby6", "source_eventfields_istandby4", "source_eventfields_istandby5", "source_eventfields_dstandby1",
                "source_eventfields_dstandby2", "source_eventfields_cstandby1", "source_eventfields_cstandby6", "source_eventfields_cstandby7",
                "source_eventfields_cstandby8", "source_eventfields_cstandby9", "source_eventfields_cstandby2", "source_eventfields_cstandby3",
                "source_eventfields_cstandby4", "source_eventfields_cstandby5");
        MapOperator<SecurityLogPojo, Row> outputData = securityLogPojoDataSource.map(s -> {
            Row row = new Row(71);
            row.setField(0, s.getIndex1());
            row.setField(1, s.getType1());
            row.setField(2, s.getId());
            row.setField(3, s.getScore());
            row.setField(4, s.getSource_lid());
            row.setField(5, s.getSource_imergecount());
            row.setField(6, s.getSource_ceventname());
            row.setField(7, s.getSource_ceventdigest());
            row.setField(8, s.getSource_ceventtype());
            row.setField(9, s.getSource_icollecttype());
            row.setField(10, s.getSource_ieventlevel());
            row.setField(11, s.getSource_iprotocol());
            row.setField(12, s.getSource_iappprotocol());
            row.setField(13, s.getSource_csrcname());
            row.setField(14, s.getSource_csrcmac());
            row.setField(15, s.getSource_csrcip());
            row.setField(16, s.getSource_csrctip());
            row.setField(17, s.getSource_isrcport());
            row.setField(18, s.getSource_isrctport());
            row.setField(19, s.getSource_cdstname());
            row.setField(20, s.getSource_cdstmac());
            row.setField(21, s.getSource_cdstip());
            row.setField(22, s.getSource_cdsttip());
            row.setField(23, s.getSource_idstport());
            row.setField(24, s.getSource_idsttport());
            row.setField(25, s.getSource_cusername());
            row.setField(26, s.getSource_cprogram());
            row.setField(27, s.getSource_coperation());
            row.setField(28, s.getSource_cobject());
            row.setField(29, s.getSource_iresult());
            row.setField(30, s.getSource_ireponse());
            row.setField(31, s.getSource_cdevname());
            row.setField(32, s.getSource_cdevtype());
            row.setField(33, s.getSource_cdevip());
            row.setField(34, s.getSource_loccurtime());
            row.setField(35, s.getSource_lrecepttime());
            row.setField(36, s.getSource_ccollectorip());
            row.setField(37, s.getSource_lsend());
            row.setField(38, s.getSource_lreceive());
            row.setField(39, s.getSource_lduration());
            row.setField(40, s.getSource_dmonitorvalue());
            row.setField(41, s.getSource_corilevel());
            row.setField(42, s.getSource_coritype());
            row.setField(43, s.getSource_crequestmsg());
            row.setField(44, s.getSource_ceventmsg());
            row.setField(45, s.getSource_eventfields_istandby2());
            row.setField(46, s.getSource_eventfields_istandby3());
            row.setField(47, s.getSource_eventfields_istandby1());
            row.setField(48, s.getSource_eventfields_cstandby11());
            row.setField(49, s.getSource_eventfields_cstandby12());
            row.setField(50, s.getSource_eventfields_cstandby10());
            row.setField(51, s.getSource_eventfields_lstandby4());
            row.setField(52, s.getSource_eventfields_lstandby3());
            row.setField(53, s.getSource_eventfields_lstandby2());
            row.setField(54, s.getSource_eventfields_lstandby1());
            row.setField(55, s.getSource_eventfields_lstandby6());
            row.setField(56, s.getSource_eventfields_lstandby5());
            row.setField(57, s.getSource_eventfields_istandby6());
            row.setField(58, s.getSource_eventfields_istandby4());
            row.setField(59, s.getSource_eventfields_istandby5());
            row.setField(60, s.getSource_eventfields_dstandby1());
            row.setField(61, s.getSource_eventfields_dstandby2());
            row.setField(62, s.getSource_eventfields_cstandby1());
            row.setField(63, s.getSource_eventfields_cstandby6());
            row.setField(64, s.getSource_eventfields_cstandby7());
            row.setField(65, s.getSource_eventfields_cstandby8());
            row.setField(66, s.getSource_eventfields_cstandby9());
            row.setField(67, s.getSource_eventfields_cstandby2());
            row.setField(68, s.getSource_eventfields_cstandby3());
            row.setField(69, s.getSource_eventfields_cstandby4());
            row.setField(70, s.getSource_eventfields_cstandby5());

            return row;
        });
        outputData.output(JDBCOutputFormat.buildJDBCOutputFormat()
                .setDrivername(driverClass)
                .setDBUrl(dbUrl)
                .setUsername(userNmae)
                .setPassword(passWord)
                .setQuery(sql)
                .finish());
        env.execute("insert csv data.");
    }


}

