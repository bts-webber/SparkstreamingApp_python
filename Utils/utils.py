import hashlib
def get_md5(s):
    md5= hashlib.md5()
    md5.update(s)
    return md5.hexdigest()
def is_chinese(s):
    s=s.decode("utf-8")
    if s>=u"\u4e00" and s<=u"\u9fa6":
        return True
    else:return False