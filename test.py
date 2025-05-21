import json

with open('config.json', 'r') as configFile:
    config = json.load(configFile)
    print(config['AgentKey'])
# 文件在退出 with 块后自动关闭