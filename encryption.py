from cryptography.fernet import Fernet
import json
import base64

import random
import string




def generate_random_key(length=32):
    characters = string.ascii_letters + string.digits + string.punctuation + string.ascii_letters + string.ascii_letters + string.ascii_letters + string.digits
    random_key = ''.join(random.choice(characters) for _ in range(length))
    return random_key


def generate_cipher_suite(key):
    code_bytes = key.encode("utf-8")
    key = base64.urlsafe_b64encode(code_bytes.ljust(32)[:32])
    cipher_suite = Fernet(key)
    return cipher_suite

def encrypt(data,key):
    data_json = json.dumps(data)
    data_bytes = data_json.encode()
    cipher_suite = generate_cipher_suite(key)
    encrypted_data = cipher_suite.encrypt(data_bytes)
    encrypted_data = str(encrypted_data.decode())
    return encrypted_data

def decrypt(encrypted_data,key):
    cipher_suite = generate_cipher_suite(key)
    encrypted_data = str(encrypted_data).encode()
    decrypted_data_bytes = cipher_suite.decrypt(encrypted_data)
    decrypted_data_json = decrypted_data_bytes.decode()
    decrypted_data = json.loads(decrypted_data_json)
    return decrypted_data



def encrypt_df(df,key):
    for i in df.columns:
        df[i] = [encrypt(x,key) for x in df[i]]
    return df

def decrypt_df(df,key):
    for i in df.columns:
        df[i] = [decrypt(x,key) for x in df[i]]
    return df


def encrypt_dict(dict,key):
    for i in dict.keys():
        dict[i] = encrypt(dict[i],key)
    return dict

def decrypt_dict(dict,key):
    for i in dict.keys():
        dict[i] = decrypt(dict[i],key)
    return dict


