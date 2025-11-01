# import asyncio, msgspec, time, os
# from datetime import date

def get_dates(from_date, num_of_days: int) -> list[tuple]:
    #from_date is of type datetime.date
    from datetime import timedelta
    
    ranges = []
    for i in range(1, num_of_days+1):
        after_date = (from_date - timedelta(days=i)).strftime("%Y/%m/%d")
        before_date = (from_date - timedelta(days=i - 1)).strftime("%Y/%m/%d")
        ranges.append((after_date, before_date))
    return ranges

# Generates service for each coroutine.
def generate_services(num: int, token_path: str, for_del: bool = False) -> list:
    """Importing libraries. """
    from googleapiclient.discovery import build
    from google.oauth2.credentials import Credentials

    SCOPES = ['https://www.googleapis.com/auth/gmail.modify']
    if for_del:
        SCOPES.append('https://mail.google.com/')

    creds = Credentials.from_authorized_user_file(token_path, SCOPES)
    services = [build('gmail', 'v1', credentials=creds, cache_discovery=False) for _ in range(num)]
    return services


# # service is synchronous method, wrapper to make it asynchronous.
# def kafka_wrapper_for_ids(service, date: tuple):
#     return service.users().messages().list(userId='me', q=f"after:{date[0]} before:{date[1]}").execute()

# # Returns list of ids for a given date range.
# async def kafka_async_get_ids(id, producer, service, date: tuple) -> list[str]:
#     start_time = time.time()
#     results = await asyncio.to_thread(kafka_wrapper_for_ids, service, date) #for type list of dictionaries(having id, thread id)
#     lst =  [dict_["id"]  for dict_ in results.get('messages', [])]
#     for ele in lst:
#         await producer.send("orders", ele)
#     await producer.send("orders", {"stop_consumer": True})
#     print(f"[S-CORO - {id}] >> Time taken: {time.time() - start_time:.4f} sec.")


# async def wrapper_for_produce(token_path: str,
#                               coro_num: int) -> None:
    
#         from aiokafka import AIOKafkaProducer

#         dates = get_dates(date.today(),coro_num)
#         services = generate_services(coro_num, token_path)    

#         producer = AIOKafkaProducer(bootstrap_servers = "kafka:9092",
#                                     value_serializer = lambda x: msgspec.msgpack.encode(x))
#         await producer.start()

#         tasks = [asyncio.create_task(kafka_async_get_ids(id=id,
#                                                 producer=producer,
#                                                 service=service,
#                                                 date=date,
#                                                 )) for id, service, date in zip(range(coro_num), services, dates)]
#         await asyncio.gather(*tasks)
#         for _ in range(18):
#             await producer.send("orders", {"stop_consumer": True})
#         await producer.flush()
#         await producer.stop()

# # service is synchronous method, wrapper to make it asynchronous.
# def wrapper_for_payload(service, message_id: str):
#     return service.users().messages().get(userId="me", id=message_id, format="full").execute()

# # Returns tuple of id, payload for a given id.
# async def async_get_payload(id, service):

#     from aiokafka import AIOKafkaConsumer

#     start_time = time.time()
#     consumer = AIOKafkaConsumer("orders",
#                                 bootstrap_servers = "kafka:9092",
#                                 value_deserializer = lambda x: msgspec.msgpack.decode(x),
#                                 group_id="orders-id",
#                                 auto_offset_reset="earliest",
#                                 enable_auto_commit=True,
#                                 session_timeout_ms=10000,      # default is 10000 (10 seconds), increase to 30 seconds
#                                 heartbeat_interval_ms=5000)
#     await consumer.start()
#     try:
#         async for msg in consumer:
#             decoded_id = msg.value
#             if isinstance(decoded_id, dict) and decoded_id.get("stop_consumer", False):
#                 print(f"🔴 Stopping Consumer [{id}] >> Time taken: {time.time() - start_time:.4f} sec.")
#                 break
#             else:
#                 await asyncio.to_thread(wrapper_for_payload, service, decoded_id)
#     finally:
#         await consumer.stop()
    
# async def wrapper_for_consumer(token_path: str, coro_num: int) -> None:
#     print("🟢 Consumer is running and subscribed to orders topic")
#     services = generate_services(coro_num, token_path)    
           
#     tasks = [asyncio.create_task(async_get_payload(id=id,
#                                                    service=service,
#                                                   )) for id, service in enumerate(services, start=1)]
#     await asyncio.gather(*tasks)
