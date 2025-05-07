from cryptography.fernet import Fernet
import base64
import os
from .config import settings
import hashlib

# 使用環境變數中的密鑰或生成一個新的密鑰
def get_key() -> bytes:
    key = settings.SECRET_KEY
    # 使用固定的鹽值來確保密鑰的一致性
    salt = b"buff_amazon_ads_integration"
    key_bytes = hashlib.pbkdf2_hmac('sha256', key.encode(), salt, 100000)
    # 轉換為 Fernet 可用的格式 (32 位元組)
    fernet_key = base64.urlsafe_b64encode(key_bytes)
    return fernet_key

# 加密函數
def encrypt_token(token: str) -> str:
    """
    加密敏感令牌
    """
    if not token:
        return ""
    
    key = get_key()
    f = Fernet(key)
    encrypted_token = f.encrypt(token.encode())
    return encrypted_token.decode()

# 解密函數
def decrypt_token(encrypted_token: str) -> str:
    """
    解密敏感令牌
    """
    if not encrypted_token:
        return ""
    
    key = get_key()
    f = Fernet(key)
    decrypted_token = f.decrypt(encrypted_token.encode())
    return decrypted_token.decode()
