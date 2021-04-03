import base64

import myTools.ModuleInstaller as mi

try:
    import cryptography.fernet as f
except:
    mi.installModule("cryptography")
    import cryptography.fernet as f


class ContentObfuscation:
    """This class should be taken as-is: it's not providing strong encryption, but merely obfuscate data to avoid having plaintext values in the RAM and displayed in a debugger
   This is not a safe way of doing content encryption.
    """
    __fernetK:bytes = b'M0tSMzdyZ083eEhkOXF3MGtydkd1Vlo0UUJwYVhlRzdlRWptQW1QbmlDbz0='

    def __init__(self: object):
        #key1=b'3KR37rgO7xHd9qw0krvGuVZ4QBpaXeG7eEjmAmPniCo='
        self._cipher_suite = f.Fernet(base64.b64decode(ContentObfuscation.__fernetK))
    

    def obfuscate(self: object, clearText: str)-> str:
        return (self._cipher_suite.encrypt(clearText.encode())).decode()

    def deObfuscate(self: object, obfuscatedText: str)-> str:
        return (self._cipher_suite.decrypt(obfuscatedText.encode())).decode()
