
#### 用例功能
本用例实现通过从 HTTP 客户端直接向 Apache Livy 端点发送请求，远程创建 pyspark 交互式会话，而无需依赖 EMR studio
1. 使用 AWS 签名协议版本 4（AWS4Signer）将身份验证信息添加到 AWS API 请求
2. 操作Livy会话，包含：创建交互式会话，查询会话，提交任务，删除会话等

**本用例仅供测试**
```
15:01:48.156 [main] DEBUG software.amazon.awssdk.auth.signer.Aws4Signer -- AWS4 String to sign: AWS4-HMAC-SHA256
20250302T070148Z
20250302/us-west-2/emr-serverless/aws4_request
e3946ce693f8d57028bdaf99eab78efdad1499ec3a51857c7d71bcbabd95c692
15:01:48.157 [main] INFO com.example.emr.EmrServerlessLivyClient -- Sending GET request: https://00fqi5n5j9ctg90l.livy.emr-serverless-services.us-west-2.amazonaws.com/sessions/2/statements/0
15:01:48.551 [main] INFO com.example.emr.EmrServerlessLivyClient -- Request successful, status code: 200
15:01:48.552 [main] INFO com.example.emr.EmrServerlessLivyClient -- Current statement state: available
15:01:48.553 [main] INFO com.example.emr.EmrServerlessLivyClient -- Statement execution completed

=== Statement Execution Result ===
{
  "id" : 0,
  "code" : "1 + 1",
  "state" : "available",
  "output" : {
    "status" : "ok",
    "execution_count" : 0,
    "data" : {
      "text/plain" : "2"
    }
  },
  "progress" : 1.0,
  "started" : 1740898907313,
  "completed" : 1740898907315
}
15:01:48.557 [main] INFO com.example.emr.EmrServerlessLivyClient -- Deleting session...
15:01:48.561 [main] DEBUG software.amazon.awssdk.auth.signer.Aws4Signer -- AWS4 Canonical Request: DELETE
/sessions/2
```

#### 使用方法
使用前请先 start EMR serverless application
```
cd emr-serverless-livy-client
mvn package
java -cp target/emr-serverless-livy-client-1.0-SNAPSHOT-jar-with-dependencies.jar com.example.emr.EmrServerlessLivyClient
```

#### 参考文档
https://docs.aws.amazon.com/zh_cn/IAM/latest/UserGuide/reference_sigv.html
https://docs.aws.amazon.com/zh_cn/emr/latest/EMR-Serverless-UserGuide/interactive-workloads-livy-endpoints.html