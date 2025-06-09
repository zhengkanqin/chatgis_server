# GeoFile/Tools/DataInputTools.py
import numpy as np
from datetime import datetime


def classify_field_type(dtype, data):
    """详细字段类型分类判断"""
    if np.issubdtype(dtype, np.floating):
        if dtype == np.float32:
            return "Float"
        return "Double"
    elif np.issubdtype(dtype, np.integer):
        min_val = data.min()
        max_val = data.max()
        if -32768 <= min_val and max_val <= 32767:
            return "Short Integer"
        return "Long Integer"
    elif np.issubdtype(dtype, np.datetime64) or isinstance(data.iloc[0], datetime):
        return "Date"
    elif dtype == object:
        return "Text"
    return "Unknown"
