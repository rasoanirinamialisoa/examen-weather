from cryptography.fernet import Fernet

key = Fernet.generate_key()
print("Clé Fernet générée :")
print(key.decode())
