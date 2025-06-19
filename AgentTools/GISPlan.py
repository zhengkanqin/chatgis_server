import json

from langchain.tools import tool
from connection_manager import manager
class GISTask:
    description:str
    state:bool
    sender:str
    feedback:str
    def __init__(self, description:str,  state:bool, sender:str, feedback:str):
        self.description = description
        self.state = state
        self.sender = sender
        self.feedback = feedback


class GISPlan:
    UserGoal:str
    TotalThinking:str
    SubTask:list[GISTask]
    Resource:list[str]
    IfUpdateTask:bool
    def __init__(self):
        self.UserGoal=""
        self.TotalThinking=""
        self.SubTask=[]
        self.Resource=[]
        self.IfUpdateTask=False

    def clear_(self):
        self.UserGoal=""
        self.TotalThinking=""
        self.SubTask=[]
        self.Resource=[]
        self.IfUpdateTask=False

    def SetUserGoal_(self,userGoal:str):
        self.UserGoal=userGoal

    def SetTotalThinking_(self,totalThinking:str):
        self.TotalThinking=totalThinking

    def GetUserGoal_(self):
        return self.UserGoal

    def GetTotalThinking_(self):
        return self.TotalThinking

    def AddSubTask_(self,task:GISTask):
        self.SubTask.append(task)
        return self.GetAllSubtask_()

    def DeleteSubTask_(self,ordination:int):
        self.SubTask = self.SubTask[:ordination]
        return self.GetAllSubtask_()

    def GetCurrentSubTask_(self) -> str:

        res_md = "## 📦 当前资源列表\n"
        if self.Resource:
            for i, res in enumerate(self.Resource, 1):
                res_md += f"{i}. {res}\n"
        else:
            res_md += "暂无资源\n"

        for task in self.SubTask:
            if not task.state:
                md = f"## ⏳ 当前执行子任务\n"
                md += f"- **任务描述**：{task.description}\n"
                md += res_md
                return md
        return "[$end]所有任务已完成[$end]"

    def GetCurrentSender_(self) -> str:
        for task in self.SubTask:
            if not task.state:
                return task.sender
        return "[$fail]系统异常，委托失败[$fail]"

    def IsUpdateTask_(self):
        print(f"当前的IsUpdateTask是：{self.IfUpdateTask}")
        if self.IfUpdateTask:
            self.IfUpdateTask = False
            return True
        else:
            return False

    def SetUpdateTask_(self):
        self.IfUpdateTask = True

    def GetAllInfo_(self) -> str:

        res_md = "## 📦 当前资源列表\n"
        if self.Resource:
            for i, res in enumerate(self.Resource, 1):
                res_md += f"{i}. {res}\n"
        else:
            res_md += "暂无资源\n"

        md = f"## 🧠 总体计划\n{self.TotalThinking}\n\n"
        md += "## 🧩 子任务列表\n"
        if not self.SubTask:
            md += "暂无子任务"
        else:
            for idx, task in enumerate(self.SubTask, 1):
                md += f"### {idx}. {task.description}\n"
                md += f"- 👤 委托者: {task.sender}\n"
                md += f"- 💬 反馈: {task.feedback}\n\n"
                md += f"- 🚦 状态: {'✅ 完成' if task.state else '🕗 未完成'}\n"
        md +=res_md
        return md

    def GetAllSubtask_(self):
        if not self.SubTask:
            return "暂无子任务"
        else:
            md = "## 🧩 当前子任务列表\n"
            for idx, task in enumerate(self.SubTask, 1):
                md += f"### {idx}. {task.description}\n"
                md += f"- 👤 委托者: {task.sender}\n"
                md += f"- 💬 反馈: {task.feedback}\n\n"
                md += f"- 🚦 状态: {'✅ 完成' if task.state else '🕗 未完成'}\n"
            return md



System_plan = GISPlan()

@tool()
def DoAddSubtask(description:str, resource:str, sender:str):
    """
    添加一条子任务到任务系统

    description(str): 任务描述
    resource(str): 任务相关文件、资源、信息、图层名
    sender(str): 委托者名称
    """
    response = System_plan.AddSubTask_(GISTask(description, False,  sender,""))
    UpdatePlanToUI()
    return response

@tool
def DoDeleteSubtask(upto_index: int):
    """
    删除某个子任务及其后所有任务（根据索引）

    upto_index(int): 保留的任务数。例如传入2，则只保留前2项（索引0和1）
    """
    response =  System_plan.DeleteSubTask_(upto_index)
    UpdatePlanToUI()
    return response

@tool
def GetAllSubtaskInfo():
    """
    获取所有子任务的详细信息
    """
    return System_plan.GetAllSubtask_()

@tool
def GetPlanFullInfo():
    """
    获取完整计划信息，包括用户目标、总体思路和所有子任务（Markdown 格式）
    """
    return System_plan.GetAllInfo_()

@tool
def ReviseSubtask(index: int, description: str = "", sender: str = ""):
    """
    修改指定索引的子任务的字段

    index(int): 待修改的子任务索引（从0开始）
    description(str): 新描述（可选）
    sender(str): 新委托人（可选）
    """
    if 0 <= index < len(System_plan.SubTask):
        task = System_plan.SubTask[index]
        if description:
            task.description = description
        if sender:
            task.sender = sender
        UpdatePlanToUI()
        return System_plan.GetAllSubtask_()
    else:
        return f"❌ 无效的任务索引：{index}"

@tool
def FinishCurrentSubtask(resource: str = "", feedback: str = ""):
    """
    完成当前未完成的子任务，并可更新资源与反馈信息

    resource(str): 新增全局资源
    feedback(str): 精简的任务执行反馈或文字形式的结果
    """
    for task in System_plan.SubTask:
        if not task.state:
            task.state = True
            if resource:
                System_plan.Resource.append(resource)
            if feedback:
                task.feedback = feedback
    UpdatePlanToUI()
    SetUpdateTask()
    return "[$end][$end]"


@tool
def FailCurrentSubtask(feedback: str = "任务失败"):
    """
    将当前未完成的任务标记为失败，添加反馈说明或资源信息

    feedback(str): 失败原因"
    """
    for task in System_plan.SubTask:
        if not task.state:
            task.feedback = feedback
    UpdatePlanToUI()
    SetUpdateTask()
    return "[$fail][$fail]"


def AreAllTasksFinished() -> bool:
    total = len(System_plan.SubTask)
    finished = sum(1 for task in System_plan.SubTask if task.state)

    if total == 0:
        return True
    elif finished == total:
        return True
    else:
        return False

def clearAll():
    System_plan.clear_()

def SetUserGoal(goal):
    System_plan.SetUserGoal_(goal)

def SetTotalThinking(Thinking):
    System_plan.SetTotalThinking_(Thinking)

def GetUserGoal():
    return System_plan.GetUserGoal_()

def GetTotalThinking():
    return System_plan.GetTotalThinking_()

def GetCurrentSender():
    return System_plan.GetCurrentSender_()

def GetCurrentSubTask():
    return System_plan.GetCurrentSubTask_()

def SetUpdateTask():
    System_plan.SetUpdateTask_()

def GetUpdateTask():
    return System_plan.IsUpdateTask_()

def GetALlSubTaskBySystem():
    return System_plan.GetAllSubtask_()

def AddPlanSource(resource:str):
    System_plan.Resource.extend(resource)

async def UpdatePlanToUI():
    ToUI = {
        "type":"plan",
        "data":GetALlSubTaskBySystem()
    }
    manager.send_message(json.dumps(ToUI))