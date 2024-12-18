"""standardize package.

standardize
"""

import numpy as np
import pandas as pd
import structlog
from bigmodule import I

# 需要安装的第三方依赖包
# from bigmodule import R
# R.require("requests>=2.0", "isort==5.13.2")

# metadata
# 模块作者
author = "BigQuant"
# 模块分类
category = "数据处理"
# 模块显示名
friendly_name = "标准化处理"
# 文档地址, optional
doc_url = "https://bigquant.com/wiki/"
# 是否自动缓存结果
cacheable = True

logger = structlog.get_logger()

def MinMaxNorm(x):
    """
    Min-Max标准化
    """
    _x = x.drop(["date", "instrument"], axis=1) if "date" in x.columns else x.drop(["instrument"], axis=1)
    arr = _x.values
    min_val = np.nanmin(arr, axis=0)
    max_val = np.nanmax(arr, axis=0)
    try:
        arr = (arr - min_val) / (max_val - min_val)
    except BaseException:
        logger.warn("数据错误，无法进行MinMax标准化")
        
    result = pd.DataFrame(arr, columns=_x.columns, index=_x.index)
    result["date"] = x["date"]
    result["instrument"] = x["instrument"]
    return result


def ZScoreNorm(x):
    """
    Z-Score标准化: 对原始数据减去均值除以标准差
    """
    _x = x.drop(["date", "instrument"], axis=1) if "date" in x.columns else x.drop(["instrument"], axis=1)
    arr = _x.values
    arr = (arr - np.nanmean(arr, axis=0)) / np.nanstd(arr, axis=0, ddof=1)
    result = pd.DataFrame(arr, columns=_x.columns, index=_x.index)
    result["date"] = x["date"]
    result["instrument"] = x["instrument"]
    return result


def RobustZScoreNorm(x):
    """Robust ZScore Normalization
    稳健Z分数标准化，即对原始数据减去中位数除以1.48倍MAD统计量

    Use robust statistics for Z-Score normalization:
        mean(x) = median(x)
        std(x) = MAD(x) * 1.4826

    Reference:
        https://en.wikipedia.org/wiki/Median_absolute_deviation.

    """
    _x = x.drop(["date","instrument"], axis=1) if "date" in x.columns else x.drop(["instrument"], axis=1)    # 只提取因子数据
    
    arr = _x.values
    EPS = 1e-12
    mean_train = np.nanmedian(arr, axis=0)
    # mad统计量
    std_train = np.nanmedian((np.abs(arr - mean_train)), axis=0)
    std_train += EPS
    std_train *= 1.4826

    arr = (arr - mean_train) / std_train

    result = pd.DataFrame(arr, columns=_x.columns, index=_x.index)
    result["date"] = x["date"]
    result["instrument"] = x["instrument"]
    return result


def CSZScoreNorm(x):
    """Cross Sectional ZScore Normalization
    截面Z分数标准化至标准正态分布
    
    Note:
        在数据标准化模块，默认是在截面数据上进行的标准化，以避免全局标准化导致的未来函数。
    """
    _x = x.drop(["date","instrument"], axis=1) if "date" in x.columns else x.drop(["instrument"], axis=1)    # 只提取因子数据
    
    arr = _x.values
    arr = (arr - np.nanmean(arr, axis=0)) / np.nanstd(arr, axis=0, ddof=1)
    
    result = pd.DataFrame(arr, columns=_x.columns, index=_x.index)
    result["date"] = x["date"]
    result["instrument"] = x["instrument"]
    return result


def CSRankNorm(x):
    """Cross Sectional Rank Normalization
    截面先转换为rank序数，再Z分数化至标准正态分布
    """
    _x = x.drop(["date","instrument"], axis=1) if "date" in x.columns else x.drop(["instrument"], axis=1)    # 只提取因子数据
    _x = _x.rank(axis=0)-1
    _x["instrument"] = x["instrument"]
    if "date" in x.columns:
        _x["date"] = x["date"]
    return ZScoreNorm(_x)


STD_FN = { "ZScoreNorm": ZScoreNorm, 
        "MinMaxNorm": MinMaxNorm,
        "RobustZScoreNorm": RobustZScoreNorm,
        "CSZScoreNorm": CSZScoreNorm,
        "CSRankNorm": CSRankNorm
        }


def run(
    input_1: I.port("输入数据", specific_type_name="DataSource"),
    input_2: I.port("因子列表", optional=True, specific_type_name="列表|DataSource") = None,
    standard_func: I.choice(
        "标准化方法", ["ZScoreNorm", "MinMaxNorm", "RobustZScoreNorm", "CSZScoreNorm", "CSRankNorm"]
    ) = "ZScoreNorm",
    columns_input: I.code("指定列", auto_complete_type="feature_fields,bigexpr_functions") = "",
)->[
    I.port("标准化数据", "data")  # type: ignore
]:
    """
    标准化处理，也可称为归一化处理，属于数据处理常见的一种方式.
    方法：
        MinMaxNorm: 最小最大值标准化至[0,1]范围
        ZScoreNorm: Z分数标准化至标准正态分布，即对原始数据减去均值除以标准差
        RobustZScoreNorm: 稳健Z分数标准化，即对原始数据减去中位数除以1.48倍MAD统计量
        CSZScoreNorm: 截面Z分数标准化至标准正态分布
        CSRankNorm: 截面先转换为rank序数，再Z分数化至标准正态分布
    """

    import dai

    input_df = input_1.read()

    if input_2 is None:
        if not columns_input:
            raise ValueError("请输入标准化的列名或连接输入因子列表模块")
        else:
            columns = [line.strip() for line in columns_input.splitlines() if line.strip() and not line.strip().startswith("#")]
    else:
        columns = input_2.read()
    
    columns += ["date", "instrument"]
    _df = input_df[columns]    # 特定数据列
    _df.replace([np.inf, -np.inf], np.nan, inplace=True)    # 缺失值过滤
    result = _df.groupby("date").apply(STD_FN[standard_func])     # 标准化处理

    # 使用标准化后的数据替换原始数据
    input_df.reset_index(drop=True, inplace=True)
    result.reset_index(drop=True, inplace=True)
    input_df[columns] = result[columns]

    ds = dai.DataSource.write_bdb(input_df)
    return I.Outputs(data=ds)


def post_run(outputs):
    """后置运行函数"""
    return outputs
