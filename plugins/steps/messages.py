from airflow.providers.telegram.hooks.telegram import TelegramHook
from dotenv import load_dotenv
import os

def send_telegram_success_message(context):
    load_dotenv()
    token_id = os.getenv("TELEGRAM_TOKEN_ID")
    chat_id = os.getenv("TELEGRAM_CHAT_ID")
    hook = TelegramHook(token=token_id, chat_id=chat_id)
    dag = context['dag']
    run_id = context['run_id']
    
    message = f'Исполнение DAG {dag} с id={run_id} прошло успешно!' # определение текста сообщения
    hook.send_message({
        'chat_id': chat_id,
        'text': message,
    })

def send_telegram_failure_message(context):
    load_dotenv()
    token_id = os.getenv("TELEGRAM_TOKEN_ID")
    chat_id = os.getenv("TELEGRAM_CHAT_ID")
    hook = TelegramHook(token=token_id, chat_id=chat_id)
    dag = context['dag']
    run_id = context['run_id']
    details = context['task_instance_key_str']
    
    message = f'При исполнении DAG {dag} с id={run_id} произошла ошибка. Детали: {details}'
    hook.send_message({
        'chat_id': chat_id,
        'text': message,
    })