UserLayers = []
system_threads_id = 1

async def Change_system_threads():
    global system_threads_id
    system_threads_id +=1
    print(f"Change_system_threads_to:{system_threads_id}")
def Get_threads():
    global system_threads_id
    print(f"Get_threads:{system_threads_id}")
    return system_threads_id