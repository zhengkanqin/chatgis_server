import re


def help_info(message):
    match = re.search(r'\[\$help](.*?)\[\$help]', message)
    if match:
        return match.group(1)
    else:
        return ""

def fail_info(message):
    match = re.search(r'\[\$fail](.*?)\[\$fail]', message)
    if match:
        return match.group(1)
    else:
        return ""

def sender_info(message):
    match = re.search(r'\[\$sender](.*?)\[\$sender]', message)
    if match:
        return match.group(1)
    else:
        return ""