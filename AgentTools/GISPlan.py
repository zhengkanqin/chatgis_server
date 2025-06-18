from langchain.tools import tool

class GISTask:
    description:str
    resource:str
    state:bool
    sender:str
    feedback:str
    def __init__(self, description:str, resource:str, state:bool, sender:str, feedback:str):
        self.description = description
        self.resource = resource
        self.state = state
        self.sender = sender
        self.feedback = feedback


class GISPlan:
    UserGoal:str
    TotalThinking:str
    SubTask:list[GISTask]

    def __init__(self):
        self.UserGoal=""
        self.TotalThinking=""
        self.SubTask=[]

    def clear_(self):
        self.UserGoal=""
        self.TotalThinking=""
        self.SubTask=[]
        return "已清空计划"

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

    def ReviseSubTask_(self,ordination:int,task:GISTask):
        self.SubTask[ordination]=task
        return self.GetAllSubtask_()

    def GetCurrentSubTask_(self) -> str:
        for task in self.SubTask:
            if not task.state:
                md = f"## ⏳ 当前执行子任务\n"
                md += f"- **任务描述**：{task.description}\n"
                md += f"- **资源链接**：{task.resource}\n"
                md += f"- **状态**：🕗 未完成\n"
                md += f"- **委托者**：{task.sender}\n"
                md += f"- **当前反馈**：{task.feedback if task.feedback else '（无反馈）'}\n"
                return md
        return "所有任务已完成"

    def FinishCurrentSubTask_(self):
        for task in self.SubTask:
            if not task.state:
                task.state = True
                return self.GetAllSubtask_()


    def GetAllInfo_(self) -> str:
        md = f"## 🧠 总体计划\n{self.TotalThinking}\n\n"
        md += "## 🧩 子任务列表\n"
        if not self.SubTask:
            md += "暂无子任务"
        else:
            for idx, task in enumerate(self.SubTask, 1):
                md += f"### {idx}. {task.description}\n"
                md += f"- 👤 委托者: {task.sender}\n"
                md += f"- 💬 反馈: {task.feedback}\n\n"
                md += f"- 📦 资源: {task.resource}\n"
                md += f"- 🚦 状态: {'✅ 完成' if task.state else '🕗 未完成'}\n"
        return md

    def GetAllSubtask_(self):
        if not self.SubTask:
            return "暂无子任务"
        else:
            md = "## 🧩 子任务列表\n"
            for idx, task in enumerate(self.SubTask, 1):
                md += f"### {idx}. {task.description}\n"
                md += f"- 👤 委托者: {task.sender}\n"
                md += f"- 💬 反馈: {task.feedback}\n\n"
                md += f"- 📦 资源: {task.resource}\n"
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
    return System_plan.AddSubTask_(GISTask(description, resource, False, sender, ""))

@tool
def DoDeleteSubtask(upto_index: int):
    """
    删除某个子任务及其后所有任务（根据索引）

    upto_index(int): 要保留的任务数。例如传入2，则只保留前2项（索引0和1）
    """
    return System_plan.DeleteSubTask_(upto_index)

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
def ReviseSubtask(index: int, description: str = "", resource: str = "", sender: str = ""):
    """
    修改指定索引的子任务的字段

    index(int): 子任务索引（从0开始）
    description(str): 新描述（可选）
    resource(str): 新资源路径或名称（可选）
    sender(str): 新委托人（可选）
    """
    if 0 <= index < len(System_plan.SubTask):
        task = System_plan.SubTask[index]
        if description:
            task.description = description
        if resource:
            task.resource = resource
        if sender:
            task.sender = sender
        return System_plan.GetAllSubtask_()
    else:
        return f"❌ 无效的任务索引：{index}"

@tool
def FinishCurrentSubtask(resource: str = "", feedback: str = ""):
    """
    完成当前未完成的子任务，并可更新资源与反馈信息

    resource(str): 可选，更新后的资源信息
    feedback(str): 可选，更新后的反馈说明
    """
    for task in System_plan.SubTask:
        if not task.state:
            task.state = True
            if resource:
                task.resource = resource
            if feedback:
                task.feedback = feedback
            return f"✅ 已完成任务：{task.description}\n\n" + System_plan.GetAllSubtask_()
    return "🎉 所有子任务已完成，无需更新。"

@tool
def FailCurrentSubtask(resource: str = "", feedback: str = "任务失败"):
    """
    将当前未完成的任务标记为失败，添加反馈说明或资源信息

    resource(str): 可选，更新资源信息
    feedback(str): 可选，说明失败原因，默认为"任务失败"
    """
    for task in System_plan.SubTask:
        if not task.state:
            task.feedback = feedback
            if resource:
                task.resource = resource
            return f"⚠️ 已标记任务失败：{task.description}\n\n" + System_plan.GetAllSubtask_()
    return "🎉 所有子任务已完成，无失败任务。"

@tool
def AreAllTasksFinished() -> str:
    """
    判断当前所有子任务是否全部完成。

    返回 Markdown 格式的自然语言说明。
    """
    total = len(System_plan.SubTask)
    finished = sum(1 for task in System_plan.SubTask if task.state)

    if total == 0:
        return "📭 当前没有任何子任务。"
    elif finished == total:
        return f"✅ 所有子任务已完成，共 {total} 项。"
    else:
        return f"⏳ 尚有未完成任务：{finished}/{total} 已完成。"

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