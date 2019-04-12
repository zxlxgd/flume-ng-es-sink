1.修改flume-ng-es-sink-1.9代码中rest client发送数据报406错误
  + httpRequest.addHeader("Content-Type","application/json");
  
2.添加x-pack支持
  + ElasticSearchTransportXPackClient.java

3.flume配置示例：
   
  a1.sinks.e1.type = org.apache.flume.sink.elasticsearch.ElasticSearchSink
  
  a1.sinks.e1.hostNames = http://elastic:XA&YtoOverseas@192.168.207.32:9200
  
  a1.sinks.e1.client = x-pack-transport
  
  a1.sinks.e1.client.securityUser = elastic:pass_wd
  
  a1.sinks.e1.client.sslKeyPath = /opt/app/apache-flume-1.9.0-bin/conf/es/ca.crt
  
  a1.sinks.e1.client.sslCertificatePath = /opt/app/apache-flume-1.9.0-bin/conf/es/elasticsearch.crt
  
  a1.sinks.e1.client.sslCertificateAuthPath = /opt/app/apache-flume-1.9.0-bin/conf/es/elasticsearch.key
  


