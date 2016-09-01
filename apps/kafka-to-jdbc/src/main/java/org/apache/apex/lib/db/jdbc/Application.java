/**
 * Put your copyright and license info here.
 */
package org.apache.apex.lib.db.jdbc;

import java.util.List;

import org.apache.apex.malhar.kafka.KafkaSinglePortInputOperator;
import org.apache.hadoop.conf.Configuration;

import com.google.common.collect.Lists;

import com.datatorrent.api.Context;
import com.datatorrent.api.DAG;
import com.datatorrent.api.Operator.ProcessingMode;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.annotation.ApplicationAnnotation;
import com.datatorrent.contrib.parser.CsvParser;
import com.datatorrent.lib.db.jdbc.JdbcPOJOInsertOutputOperator;
import com.datatorrent.lib.db.jdbc.JdbcTransactionalStore;
import com.datatorrent.lib.util.FieldInfo.SupportType;

@ApplicationAnnotation(name = "kafka-to-jdbc")
public class Application implements StreamingApplication
{

  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    KafkaSinglePortInputOperator kafka = dag.addOperator("KafkaInput", new KafkaSinglePortInputOperator());

    CsvParser parser = dag.addOperator("CSVParser", new CsvParser());

    JdbcPOJOInsertOutputOperator jdbc = dag.addOperator("JdbcOutput", new JdbcPOJOInsertOutputOperator());
    JdbcTransactionalStore outputStore = new JdbcTransactionalStore();
    jdbc.setStore(outputStore);
    jdbc.setFieldInfos(addJdbcFieldInfos());

    dag.addStream("Kafka2Parser", kafka.outputPort, parser.in);
    dag.addStream("Parser2JDBC", parser.out, jdbc.input);
//    dag.setAttribute(kafka, Context.OperatorContext.PROCESSING_MODE, ProcessingMode.EXACTLY_ONCE);
    dag.setAttribute(parser, Context.OperatorContext.PROCESSING_MODE, ProcessingMode.EXACTLY_ONCE);
    dag.setAttribute(jdbc, Context.OperatorContext.PROCESSING_MODE, ProcessingMode.AT_MOST_ONCE);
  }

  private List<com.datatorrent.lib.db.jdbc.JdbcFieldInfo> addJdbcFieldInfos()
  {
    List<com.datatorrent.lib.db.jdbc.JdbcFieldInfo> fieldInfos = Lists.newArrayList();

    fieldInfos
        .add(new com.datatorrent.lib.db.jdbc.JdbcFieldInfo("customerName", "customerName", SupportType.STRING, 0));
    fieldInfos.add(new com.datatorrent.lib.db.jdbc.JdbcFieldInfo("customerPhone", "customerPhone", SupportType.STRING,
        0));
    fieldInfos.add(new com.datatorrent.lib.db.jdbc.JdbcFieldInfo("customerEmail", "customerEmail", SupportType.STRING,
        0));
    fieldInfos.add(new com.datatorrent.lib.db.jdbc.JdbcFieldInfo("city", "city", SupportType.STRING, 0));
    fieldInfos.add(new com.datatorrent.lib.db.jdbc.JdbcFieldInfo("country", "country", SupportType.STRING, 0));
    fieldInfos.add(new com.datatorrent.lib.db.jdbc.JdbcFieldInfo("uid", "uid", SupportType.STRING, 0));
    fieldInfos.add(new com.datatorrent.lib.db.jdbc.JdbcFieldInfo("accountNumber", "accountNumber", SupportType.STRING,
        0));

    fieldInfos.add(new com.datatorrent.lib.db.jdbc.JdbcFieldInfo("txId", "txId", SupportType.LONG, 0));
    //      fieldInfos.add(new com.datatorrent.lib.db.jdbc.JdbcFieldInfo("txDate", "txDate", SupportType.LONG,0));
    fieldInfos.add(new com.datatorrent.lib.db.jdbc.JdbcFieldInfo("txAmount", "txAmount", SupportType.DOUBLE, 0));
    //      fieldInfos.add(new com.datatorrent.lib.db.jdbc.JdbcFieldInfo("label", "label", SupportType.STRING,0));
    //      fieldInfos.add(new com.datatorrent.lib.db.jdbc.JdbcFieldInfo("state", "state", SupportType.STRING,0));
    return fieldInfos;
  }
}
