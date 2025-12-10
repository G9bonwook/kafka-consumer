from confluent_kafka import Consumer
from confluent_kafka import KafkaError, KafkaException
from consumers.base_consumer import BaseConsumer
import pandas as pd
import sys
import json
import time

class ConsumeConsumer(BaseConsumer):
    def __init__(self, group_id):
        super().__init__(group_id)
        # 구독할 Kafka 토픽 리스트 정의
        self.topics = ['apis.seouldata.rt-bicycle']

        # Consumer 설정
        conf = {'bootstrap.servers': self.BOOTSTRAP_SERVERS, # 카프카 브로커 주소
                'group.id': self.group_id, # 컨슈머 그룹 ID
                'auto.offset.reset': 'earliest', # 오프셋 정보가 없을 때 처음부터 읽기
                'enable.auto.commit': 'false' # 자동 커밋 끄기
                }

        # Consumer 인스턴스 생성 및 구독
        self.consumer = Consumer(conf)
        self.consumer.subscribe(self.topics, on_assign=self.callback_on_assign)


    def poll(self):
        try:
            while True:
                # 한 번에 최대 100개의 메시지를 리스트로 가져옴
                # poll()은 한 개씩, consume()은 배치 처리
                msg_lst = self.consumer.consume(num_messages=100)
                if msg_lst is None or len(msg_lst) == 0: continue

                self.logger.info(f'message count:{len(msg_lst)}')
                for msg in msg_lst:
                    error = msg.error()
                    if error:
                        self.handle_error(msg, error)

                # 로직 처리 부분
                # Kafka 레코드에 대한 전처리, Target Sink 등 수행
                self.logger.info(f'message 처리 로직 시작')
                msg_val_lst = [json.loads(msg.value().decode('utf-8')) for msg in msg_lst]
                df = pd.DataFrame(msg_val_lst)
                print(df[:10])


                self.logger.info(f'message 처리 로직 완료, Async Commit 후 2초 대기')
                # 로직 처리 완료 후 Async Commit 수행
                self.consumer.commit(asynchronous=True)
                self.logger.info(f'Commit 완료')
                time.sleep(2)

        except KafkaException as e:
            self.logger.exception("Kafka exception occurred during message consumption")

        except KeyboardInterrupt: # Ctrl + C 눌러 종료시
            self.logger.info("Shutting down consumer due to keyboard interrupt.")

        finally:
            self.consumer.close()
            self.logger.info("Consumer closed.")


if __name__ == '__main__':
    consume_consumer= ConsumeConsumer('consume_consumer')
    consume_consumer.poll()