from botocore import crt
import requests 
from botocore.awsrequest import AWSRequest
import botocore.session
import json, pprint, textwrap
import time
import logging
import sys

# 设置基本日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger('emr-serverless')

class EMRServerlessClient:
    def __init__(self, application_id, region, role_arn, timeout=300):
        """
        初始化EMR Serverless客户端
        
        参数:
            application_id: EMR Serverless应用ID
            region: AWS区域
            role_arn: 执行角色ARN
            timeout: 操作超时时间(秒)
        """
        self.endpoint = f'https://{application_id}.livy.emr-serverless-services.{region}.amazonaws.com'
        self.headers = {'Content-Type': 'application/json'}
        self.role_arn = role_arn
        self.timeout = timeout
        self.session_url = None
        
        # 初始化AWS会话和签名器
        try:
            session = botocore.session.Session()
            self.signer = crt.auth.CrtS3SigV4Auth(session.get_credentials(), 'emr-serverless', region)
            logger.info(f"已初始化EMR Serverless客户端，端点: {self.endpoint}")
        except Exception as e:
            logger.error(f"初始化失败: {str(e)}")
            raise
    
    def _send_request(self, method, path, data=None, max_retries=3):
        """发送已签名的请求到EMR Serverless"""
        url = f"{self.endpoint}{path}"
        logger.info(f"发送{method}请求: {url}")
        
        if data:
            logger.debug(f"请求数据: {json.dumps(data, indent=2)}")
        
        retry_count = 0
        while retry_count < max_retries:
            try:
                # 创建并签名请求
                request = AWSRequest(
                    method=method, 
                    url=url, 
                    data=json.dumps(data) if data else None, 
                    headers=self.headers
                )
                request.context["payload_signing_enabled"] = False
                self.signer.add_auth(request)
                prepped = request.prepare()
                
                # 发送请求
                if method == 'GET':
                    response = requests.get(prepped.url, headers=prepped.headers)
                elif method == 'POST':
                    response = requests.post(prepped.url, headers=prepped.headers, data=json.dumps(data) if data else None)
                elif method == 'DELETE':
                    response = requests.delete(prepped.url, headers=prepped.headers)
                else:
                    raise ValueError(f"不支持的HTTP方法: {method}")
                
                # 检查响应
                if 200 <= response.status_code < 300:
                    logger.info(f"请求成功，状态码: {response.status_code}")
                    return response
                else:
                    logger.error(f"请求失败，状态码: {response.status_code}, 响应: {response.text}")
                    # 对于某些错误码可能需要重试
                    if response.status_code in [429, 500, 502, 503, 504]:
                        retry_count += 1
                        wait_time = 2 ** retry_count  # 指数退避
                        logger.info(f"等待{wait_time}秒后重试...")
                        time.sleep(wait_time)
                        continue
                    return response
                    
            except requests.exceptions.RequestException as e:
                logger.error(f"请求异常: {str(e)}")
                retry_count += 1
                if retry_count < max_retries:
                    wait_time = 2 ** retry_count
                    logger.info(f"等待{wait_time}秒后重试...")
                    time.sleep(wait_time)
                else:
                    logger.error(f"达到最大重试次数({max_retries})，放弃请求")
                    raise
        
        return None
    
    def create_session(self):
        """创建一个新的Spark会话"""
        logger.info("创建新的Spark会话...")
        
        data = {
            'kind': 'pyspark', 
            'heartbeatTimeoutInSecond': 60, 
            'conf': { 
                'emr-serverless.session.executionRoleArn': self.role_arn
            }
        }
        
        response = self._send_request('POST', "/sessions", data)
        if not response or response.status_code >= 300:
            raise Exception(f"创建会话失败: {response.text if response else '无响应'}")
        
        # 保存会话URL
        self.session_url = response.headers.get('location')
        if not self.session_url:
            raise Exception("响应中没有会话位置信息")
        
        logger.info(f"会话创建成功，位置: {self.session_url}")
        return response.json()
    
    def wait_for_session_ready(self, poll_interval=5):
        """等待会话变为就绪状态"""
        if not self.session_url:
            raise Exception("没有活动的会话")
        
        logger.info("等待会话就绪...")
        start_time = time.time()
        
        while time.time() - start_time < self.timeout:
            response = self._send_request('GET', self.session_url)
            if not response:
                logger.error("获取会话状态失败")
                time.sleep(poll_interval)
                continue
                
            session_state = response.json().get('state', 'unknown')
            logger.info(f"当前会话状态: {session_state}")
            
            if session_state == 'idle':
                logger.info("会话已就绪")
                return True
            elif session_state in ['error', 'dead', 'killed']:
                logger.error(f"会话进入错误状态: {session_state}")
                return False
                
            logger.info(f"会话尚未就绪，等待{poll_interval}秒...")
            time.sleep(poll_interval)
        
        logger.error(f"等待会话就绪超时({self.timeout}秒)")
        return False
    
    def list_sessions(self):
        """列出所有会话"""
        logger.info("获取会话列表...")
        response = self._send_request('GET', "/sessions")
        if not response or response.status_code >= 300:
            logger.error("获取会话列表失败")
            return None
        return response.json()
    
    def submit_statement(self, code):
        """提交代码语句到会话"""
        if not self.session_url:
            raise Exception("没有活动的会话")
            
        logger.info(f"提交代码语句: {code}")
        data = {'code': code}
        
        statements_url = f"{self.session_url}/statements"
        response = self._send_request('POST', statements_url, data)
        
        if not response or response.status_code >= 300:
            raise Exception(f"提交语句失败: {response.text if response else '无响应'}")
            
        statement_location = response.headers.get('location')
        if not statement_location:
            raise Exception("响应中没有语句位置信息")
            
        logger.info(f"语句提交成功，位置: {statement_location}")
        return statement_location, response.json()
    
    def get_statement_result(self, statement_location, poll_interval=2):
        """获取语句执行结果，等待直到完成"""
        logger.info("等待语句执行完成...")
        start_time = time.time()
        
        while time.time() - start_time < self.timeout:
            response = self._send_request('GET', statement_location)
            if not response:
                logger.error("获取语句状态失败")
                time.sleep(poll_interval)
                continue
                
            statement_data = response.json()
            statement_state = statement_data.get('state', 'unknown')
            logger.info(f"当前语句状态: {statement_state}")
            
            if statement_state == 'available':
                logger.info("语句执行完成")
                return statement_data
            elif statement_state in ['error', 'cancelled']:
                logger.error(f"语句执行失败: {statement_state}")
                return statement_data
                
            logger.info(f"语句执行中，等待{poll_interval}秒...")
            time.sleep(poll_interval)
        
        logger.error(f"等待语句执行超时({self.timeout}秒)")
        return None
    
    def delete_session(self):
        """删除当前会话"""
        if not self.session_url:
            logger.warning("没有活动的会话可删除")
            return False
            
        logger.info("删除会话...")
        response = self._send_request('DELETE', self.session_url)
        
        if not response or response.status_code >= 300:
            logger.error(f"删除会话失败: {response.text if response else '无响应'}")
            return False
            
        logger.info("会话删除成功")
        self.session_url = None
        return True


def main():
    """示例用法"""
    # 替换为您的实际值
    application_id = "00fqi5n5j9ctg90l"
    region = "us-west-2"
    role_arn = "arn:aws:iam::808577411626:role/emrServerlessLivyTestRole"
    
    try:
        # 创建客户端
        client = EMRServerlessClient(application_id, region, role_arn)
        
        # 创建会话
        session_info = client.create_session()
        print("\n=== 创建的会话信息 ===")
        pprint.pprint(session_info)
        
        # 等待会话就绪
        if not client.wait_for_session_ready():
            print("会话未能就绪，退出")
            client.delete_session()
            return
        
        # 列出会话
        sessions = client.list_sessions()
        print("\n=== 会话列表 ===")
        pprint.pprint(sessions)
        
        # 提交简单计算
        statement_location, statement_info = client.submit_statement("1 + 1")
        print("\n=== 提交的语句信息 ===")
        pprint.pprint(statement_info)
        
        # 获取结果
        result = client.get_statement_result(statement_location)
        print("\n=== 语句执行结果 ===")
        pprint.pprint(result)
        
        # 提交更复杂的计算
        statement_location, _ = client.submit_statement("""
        data = [2, 2, 4, 8, 8, 8, 8, 16, 32, 32]
        rdd = sc.parallelize(data)
        """)
        
        # 获取结果
        result = client.get_statement_result(statement_location)
        print("\n=== 复杂语句执行结果 ===")
        pprint.pprint(result)
        
    except Exception as e:
        print(f"错误: {str(e)}")
    finally:
        # 确保会话被删除
        if 'client' in locals() and client.session_url:
            client.delete_session()
            print("已清理会话资源")


if __name__ == "__main__":
    main()