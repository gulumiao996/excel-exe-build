# -*- coding: utf-8 -*-
"""
素材人效日汇总_v2.3.1.py
修复：Sheet3 列选择用了 set，改为 list，并做缺列兜底。
其余逻辑同 v2.3。
"""
import os, re, glob, sys, traceback
from datetime import datetime
import numpy as np
import pandas as pd

OUT_XLSX = "素材人效日汇总_v2.3.1.xlsx"
LOG_FILE = "run_log.txt"

class TeeLogger:
    def __init__(self, logfile):
        self.terminal = sys.stdout
        self.log = open(logfile, "a", encoding="utf-8")
    def write(self, message):
        self.terminal.write(message)
        self.log.write(message)
        self.log.flush()
    def flush(self):
        self.terminal.flush()
        self.log.flush()

sys.stdout = TeeLogger(LOG_FILE)

def now_str():
    from datetime import datetime
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

def info(msg):
    print(f"[{now_str()}] {msg}")

def read_excel_df(fp: str, sheet_name=0) -> pd.DataFrame:
    return pd.read_excel(fp, sheet_name=sheet_name, dtype=str)

def read_excel_any(fp: str) -> pd.DataFrame:
    raw = pd.read_excel(fp, sheet_name=None, dtype=str)
    if "Sheet1" in raw and isinstance(raw["Sheet1"], pd.DataFrame):
        df = raw["Sheet1"]
        if len(df.columns)>0:
            return df
    for name, df in raw.items():
        if isinstance(df, pd.DataFrame) and len(df.columns)>0 and len(df.dropna(how="all"))>0:
            return df
    first = list(raw.values())[0]
    return first if isinstance(first, pd.DataFrame) else pd.DataFrame()

def read_csv_any(fp: str) -> pd.DataFrame:
    for enc in ("utf-8-sig","utf-8","gbk","gb18030"):
        try:
            return pd.read_csv(fp, encoding=enc, dtype=str, low_memory=False)
        except Exception:
            pass
    return pd.read_csv(fp, dtype=str, low_memory=False)

def to_date_str(x, fmt_out="%Y-%m-%d"):
    if pd.isna(x):
        return None
    s = str(x).strip()
    if not s:
        return None
    for fmt in ("%Y-%m-%d","%Y/%m/%d","%Y.%m.%d",
                "%Y-%m-%d %H:%M:%S","%Y/%m/%d %H:%M:%S",
                "%Y-%m-%d %H:%M","%Y/%m/%d %H:%M"):
        try:
            from datetime import datetime
            dt = datetime.strptime(s, fmt)
            return dt.strftime(fmt_out)
        except Exception:
            continue
    try:
        dt = pd.to_datetime(s, errors="coerce")
        if pd.isna(dt):
            return None
        return dt.strftime(fmt_out)
    except Exception:
        return None

import re as _re
def to_float(x):
    if x is None or (isinstance(x, float) and np.isnan(x)):
        return 0.0
    s = str(x).strip().replace(",", "")
    if s == "":
        return 0.0
    if s.endswith("%"):
        try:
            return float(s.strip("%"))/100.0
        except Exception:
            return 0.0
    try:
        return float(s)
    except Exception:
        try:
            return float(_re.sub(r"[^0-9\.\-]", "", s) or 0)
        except Exception:
            return 0.0

def non_empty(val):
    s = str(val).strip().lower()
    return s not in ("", "nan", "none", "-", "null", "无")

def normalize_team(x):
    if pd.isna(x): return ""
    s = str(x).strip()
    if not s: return ""
    names = [n.strip() for n in s.split(",") if n.strip()!=""]
    if not names: return ""
    names = sorted(names, key=lambda z: z)
    return ", ".join(names)

def _winsor_minmax_norm(s: pd.Series, p_low=5, p_high=95):
    s = pd.to_numeric(s, errors="coerce").fillna(0.0)
    if len(s)==0:
        return pd.Series([], dtype=float)
    lo = float(np.nanpercentile(s, p_low))
    hi = float(np.nanpercentile(s, p_high))
    if hi < lo:
        lo, hi = hi, lo
    s_clip = s.clip(lower=lo, upper=hi)
    denom = (hi - lo)
    if denom == 0 or np.isclose(denom, 0):
        return pd.Series(np.zeros(len(s)), index=s.index, dtype=float)
    return (s_clip - lo) / denom

def compute_expo_threshold(expo_series: pd.Series) -> float:
    s = pd.to_numeric(expo_series, errors="coerce").fillna(0.0)
    s = s[s>0]
    if len(s)==0:
        return 5000.0
    p90 = float(np.nanpercentile(s, 90))
    return max(5000.0, p90)

def compute_percent_score(df: pd.DataFrame, expo_col="整体展现次数", expo_threshold: float = 10000.0):
    weights = {
        "roi": 0.35,
        "消耗金额": 0.25,
        "平均点击率": 0.15,
        "平均转化率": 0.15,
        "平均 3s 完播率": 0.05,
        "成交金额": 0.05,
    }
    out = df.copy()
    out["消耗金额"] = pd.to_numeric(out.get("消耗金额", 0.0), errors="coerce").fillna(0.0).clip(lower=0.0)
    out["成交金额"] = pd.to_numeric(out.get("成交金额", 0.0), errors="coerce").fillna(0.0).clip(lower=0.0)
    out["roi"] = out.apply(lambda r: (r["成交金额"]/r["消耗金额"]) if r["消耗金额"]>0 else 0.0, axis=1)

    n_roi   = _winsor_minmax_norm(out["roi"])
    n_spend = _winsor_minmax_norm(np.log1p(out["消耗金额"]))
    n_ctr   = _winsor_minmax_norm(pd.to_numeric(out.get("平均点击率", 0.0), errors="coerce"))
    n_cvr   = _winsor_minmax_norm(pd.to_numeric(out.get("平均转化率", 0.0), errors="coerce"))
    n_v3s   = _winsor_minmax_norm(pd.to_numeric(out.get("平均 3s 完播率", 0.0), errors="coerce"))
    n_gmv   = _winsor_minmax_norm(out["成交金额"])

    score01 = (
        weights["roi"]            * n_roi   +
        weights["消耗金额"]        * n_spend +
        weights["平均点击率"]      * n_ctr   +
        weights["平均转化率"]      * n_cvr   +
        weights["平均 3s 完播率"]  * n_v3s   +
        weights["成交金额"]        * n_gmv
    )

    expo = pd.to_numeric(out.get(expo_col, 0.0), errors="coerce").fillna(0.0).clip(lower=0.0)
    expo_threshold = float(expo_threshold) if expo_threshold and expo_threshold>0 else 10000.0
    reliability = np.sqrt(expo / expo_threshold).clip(0.0, 1.0)

    out["评分"] = np.round(100.0 * score01 * reliability, 1)
    return out

def load_register() -> pd.DataFrame:
    files = [f for f in ["千川素材ID登记表.xlsx"] if os.path.exists(f)]
    if not files:
        raise FileNotFoundError("未找到『千川素材ID登记表.xlsx』")
    df = read_excel_any(files[0])
    df.columns = [str(c).strip() for c in df.columns]

    cols = df.columns.tolist()
    id_col = next((c for c in cols if "千川素材ID" in c), None)
    col_bd = next((c for c in cols if "编导" in c), None)
    col_ps = next((c for c in cols if "拍摄" in c), None)
    col_jj = next((c for c in cols if "剪辑" in c), None)
    if not id_col:
        id_col = cols[0]

    reg = pd.DataFrame({
        "千川素材ID": df[id_col].astype(str).str.strip(),
        "编导": df[col_bd] if col_bd in df.columns else "",
        "拍摄": df[col_ps] if col_ps in df.columns else "",
        "剪辑": df[col_jj] if col_jj in df.columns else "",
    })
    reg["拍摄"] = reg["拍摄"].map(normalize_team)
    reg = reg[ reg["千川素材ID"].apply(non_empty) ].drop_duplicates(subset=["千川素材ID"])
    return reg

def explode_mapping(reg: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for _, r in reg.iterrows():
        mid = str(r["千川素材ID"]).strip()
        for role_col, role_name in [("编导","编导"),("拍摄","拍摄"),("剪辑","剪辑")]:
            val = r.get(role_col, "")
            if pd.isna(val) or str(val).strip()=="":
                continue
            names = [n.strip() for n in str(val).split(",") if n.strip()!=""]
            for nm in names:
                rows.append({"千川素材ID": mid, "姓名": nm, "岗位": role_name})
    if not rows:
        return pd.DataFrame(columns=["千川素材ID","姓名","岗位"])
    m = pd.DataFrame(rows).drop_duplicates()
    return m

def _find_col_gmv(cols):
    for c in cols:
        s = str(c)
        if ("成交金额" in s) and ("率" not in s):
            return c
    for key in ["整体成交金额","GMV","成交金额（元）","成交金额(元)","成交金额"]:
        for c in cols:
            if key in str(c):
                return c
    return None

def load_material_data() -> pd.DataFrame:
    files = sorted(glob.glob("全域推广数据-投后数据-素材-*.xlsx")+glob.glob("全域推广数据-投后数据-素材-*.csv"))
    if not files:
        raise FileNotFoundError("未找到『全域推广数据-投后数据-素材-*.xlsx/.csv』文件")
    rows = []
    for fp in files:
        try:
            if fp.lower().endswith(".csv"):
                df = read_csv_any(fp)
            else:
                try:
                    df = read_excel_df(fp, sheet_name="Sheet1")
                except Exception:
                    df = read_excel_any(fp)
            cols = [str(c).strip() for c in df.columns]
            df.columns = cols

            col_date = next((c for c in cols if "日期" in c), None)
            col_id   = next((c for c in cols if "素材ID" in c or ("素材" in c and "ID" in c)), None)
            col_cost = next((c for c in cols if "整体消耗" in c or c=="消耗"), None)
            col_gmv  = _find_col_gmv(cols)
            col_ctim = next((c for c in cols if "素材创建时间" in c or "创建" in c), None)
            col_v3s  = next((c for c in cols if "3秒播放率" in c), None)
            col_ctr  = next((c for c in cols if "整体点击率" in c or "点击率" in c), None)
            col_cvr  = next((c for c in cols if "整体转化率" in c or "转化率" in c), None)
            col_expo = next((c for c in cols if "展现" in c), None)

            need = [col_date,col_id,col_cost,col_ctim,col_v3s,col_ctr,col_cvr]
            if any(c is None for c in need):
                missing = [n for n in need if n is None]
                raise ValueError(f"必需列缺失：{missing} in {fp}")

            gmv_source = None
            if col_gmv is None:
                if df.shape[1] >= 15:
                    col_gmv = df.columns[14]
                    gmv_source = f"(fallback O列: {col_gmv})"
                else:
                    raise ValueError(f"未找到成交金额列，且无法按O列回退（列数={df.shape[1]}） in {fp}")
            else:
                gmv_source = f"(header: {col_gmv})"

            if col_expo is None:
                df["__expo_zero__"] = 0
                col_expo = "__expo_zero__"

            t = pd.DataFrame({
                "日期": df[col_date].map(lambda x: to_date_str(x)),
                "千川素材ID": df[col_id].astype(str).str.strip(),
                "整体消耗": df[col_cost].map(to_float),
                "整体成交金额": df[col_gmv].map(to_float),
                "素材创建日期": df[col_ctim].map(lambda x: to_date_str(x)),
                "3秒播放率": df[col_v3s].map(to_float),
                "整体点击率": df[col_ctr].map(to_float),
                "整体转化率": df[col_cvr].map(to_float),
                "整体展现次数": pd.to_numeric(df[col_expo], errors="coerce").fillna(0.0),
            })
            raw_sum = float(pd.to_numeric(df[col_gmv].map(to_float), errors="coerce").fillna(0.0).sum())
            use_sum = float(t["整体成交金额"].sum())
            info(f"读取：{os.path.basename(fp)} 行={len(t)}  成交列={gmv_source}  文件内总成交={raw_sum:.2f}  读取后总成交={use_sum:.2f}")

            rows.append(t)
        except Exception as e:
            info(f"⚠️ 读取失败：{fp}  {e}")
    if not rows:
        raise RuntimeError("素材明细读取为空")
    mat = pd.concat(rows, ignore_index=True)
    mat = mat[ mat["日期"].apply(non_empty) & mat["千川素材ID"].apply(non_empty) ]
    return mat

def build_daily_person_role(mat: pd.DataFrame, mapping: pd.DataFrame) -> pd.DataFrame:
    daily_mid = (mat.groupby(["日期","千川素材ID"], as_index=False).agg({
        "整体消耗":"sum",
        "整体成交金额":"sum",
        "整体展现次数":"sum",
        "3秒播放率":"mean",
        "整体点击率":"mean",
        "整体转化率":"mean",
        "素材创建日期":"first"
    }))
    df = daily_mid.merge(mapping, on="千川素材ID", how="left")
    df = df[ df["姓名"].apply(non_empty) & df["岗位"].apply(non_empty) ]

    new_upload = (df.loc[df["素材创建日期"]==df["日期"], ["日期","姓名","岗位","千川素材ID"]]
                    .drop_duplicates()
                    .groupby(["日期","姓名","岗位"], as_index=False)["千川素材ID"].count()
                    .rename(columns={"千川素材ID":"新上传作品数"}))

    thresholds = [0,1000,10000,30000,50000,100000]
    th_rows = []
    for _, r in df.iterrows():
        base = {"日期": r["日期"], "姓名": r["姓名"], "岗位": r["岗位"], "千川素材ID": r["千川素材ID"]}
        for t in thresholds:
            base[f"消耗＞{t} 作品数"] = 1 if r["整体消耗"]>t else 0
        th_rows.append(base)
    th_df = pd.DataFrame(th_rows).drop_duplicates(subset=["日期","姓名","岗位","千川素材ID"])
    th_df = th_df.groupby(["日期","姓名","岗位"], as_index=False).sum()

    sums = (df.groupby(["日期","姓名","岗位"], as_index=False)
              .agg({"整体消耗":"sum","整体成交金额":"sum","整体展现次数":"sum"})
              .rename(columns={"整体消耗":"消耗金额","整体成交金额":"成交金额"}))

    rates = df[df["整体消耗"]>0].groupby(["日期","姓名","岗位"], as_index=False).agg({
        "3秒播放率":"mean",
        "整体点击率":"mean",
        "整体转化率":"mean"
    }).rename(columns={"3秒播放率":"平均 3s 完播率","整体点击率":"平均点击率","整体转化率":"平均转化率"})

    out = sums.merge(new_upload, on=["日期","姓名","岗位"], how="left") \
              .merge(th_df, on=["日期","姓名","岗位"], how="left") \
              .merge(rates, on=["日期","姓名","岗位"], how="left")

    for c in ["新上传作品数"]+[f"消耗＞{t} 作品数" for t in thresholds]:
        if c in out.columns: out[c] = out[c].fillna(0).astype(int)
    for c in ["平均 3s 完播率","平均点击率","平均转化率"]:
        if c in out.columns: out[c] = out[c].fillna(0.0)
    return out

def build_material_person_role(mat: pd.DataFrame, mapping: pd.DataFrame) -> pd.DataFrame:
    daily_mid = (mat.groupby(["日期","千川素材ID"], as_index=False).agg({
        "整体消耗":"sum",
        "整体成交金额":"sum",
        "整体展现次数":"sum",
        "3秒播放率":"mean",
        "整体点击率":"mean",
        "整体转化率":"mean",
        "素材创建日期":"first"
    }))
    df = daily_mid.merge(mapping, on="千川素材ID", how="left")
    df = df[ df["姓名"].apply(non_empty) & df["岗位"].apply(non_empty) ]

    sums = (df.groupby(["千川素材ID","姓名","岗位"], as_index=False)
              .agg({"整体消耗":"sum","整体成交金额":"sum","整体展现次数":"sum"})
              .rename(columns={"整体消耗":"消耗金额","整体成交金额":"成交金额"}))

    new_upl = (
        df.loc[df["素材创建日期"] == df["日期"], ["千川素材ID", "姓名", "岗位"]]
          .drop_duplicates()
          .groupby(["千川素材ID", "姓名", "岗位"], as_index=False)
          .size()
          .rename(columns={"size": "新上传作品数"})
    )

    thresholds = [0,1000,10000,30000,50000,100000]
    th_rows = []
    for _, r in df.iterrows():
        base = {"千川素材ID": r["千川素材ID"], "姓名": r["姓名"], "岗位": r["岗位"], "日期": r["日期"], "整日消耗": r["整体消耗"]}
        for t in thresholds:
            base[f"消耗＞{t} 作品数"] = 1 if r["整体消耗"]>t else 0
        th_rows.append(base)
    th_df = pd.DataFrame(th_rows).drop_duplicates(subset=["千川素材ID","姓名","岗位","日期"])
    th_sum = th_df.groupby(["千川素材ID","姓名","岗位"], as_index=False).sum().drop(columns=["日期","整日消耗"], errors="ignore")

    rates = df[df["整体消耗"]>0].groupby(["千川素材ID","姓名","岗位"], as_index=False).agg({
        "3秒播放率":"mean",
        "整体点击率":"mean",
        "整体转化率":"mean"
    }).rename(columns={"3秒播放率":"平均 3s 完播率","整体点击率":"平均点击率","整体转化率":"平均转化率"})

    out = sums.merge(new_upl, on=["千川素材ID","姓名","岗位"], how="left") \
              .merge(th_sum, on=["千川素材ID","姓名","岗位"], how="left") \
              .merge(rates, on=["千川素材ID","姓名","岗位"], how="left")

    for c in ["新上传作品数"]+[f"消耗＞{t} 作品数" for t in thresholds]:
        if c in out.columns: out[c] = out[c].fillna(0).astype(int)
    for c in ["平均 3s 完播率","平均点击率","平均转化率"]:
        if c in out.columns: out[c] = out[c].fillna(0.0)
    return out

def main():
    try:
        info("══ 扫描与读取 ──")
        reg = load_register()
        info(f"登记表：{len(reg)} 行")

        mapping = explode_mapping(reg)
        info(f"映射行：{len(mapping)} 行（素材ID-姓名-岗位）")

        mat = load_material_data()
        info(f"素材明细：{len(mat)} 行")

        all_dates = pd.to_datetime(mat["日期"], errors="coerce").dropna()
        date_span = f"{all_dates.min():%m%d}-{all_dates.max():%m%d}" if len(all_dates)>0 else ""

        expo_threshold = compute_expo_threshold(mat["整体展现次数"])
        info(f"自动曝光门槛（阈值）：{expo_threshold:.0f}")

        info("══ 构建 Sheet1（日汇总） ──")
        sheet1 = build_daily_person_role(mat, mapping)
        sheet1 = compute_percent_score(sheet1, expo_col="整体展现次数", expo_threshold=expo_threshold)
        ordered1 = ["日期","姓名","岗位","消耗金额","成交金额","roi","新上传作品数",
                    "消耗＞0 作品数","消耗＞1000 作品数","消耗＞10000 作品数",
                    "消耗＞30000 作品数","消耗＞50000 作品数","消耗＞100000 作品数",
                    "整体展现次数","平均 3s 完播率","平均点击率","平均转化率","评分"]
        for c in ordered1:
            if c not in sheet1.columns: sheet1[c] = 0
        sheet1 = sheet1[ordered1].sort_values(["日期","姓名","岗位"]).reset_index(drop=True)

        info("══ 构建 Sheet2（素材评分明细） ──")
        sheet2 = build_material_person_role(mat, mapping)
        sheet2 = compute_percent_score(sheet2, expo_col="整体展现次数", expo_threshold=expo_threshold)
        sheet2.insert(0, "汇总日期", date_span)
        ordered2 = ["汇总日期","千川素材ID","姓名","岗位","消耗金额","成交金额","roi","新上传作品数",
                    "消耗＞0 作品数","消耗＞1000 作品数","消耗＞10000 作品数",
                    "消耗＞30000 作品数","消耗＞50000 作品数","消耗＞100000 作品数",
                    "整体展现次数","平均 3s 完播率","平均点击率","平均转化率","评分"]
        for c in ordered2:
            if c not in sheet2.columns: sheet2[c] = 0
        sheet2 = sheet2[ordered2].sort_values(["评分","消耗金额","成交金额"], ascending=[False, False, False]).reset_index(drop=True)

        info("══ 构建 Sheet3（登记表+评分） ──")
        avg_score = sheet2.groupby("千川素材ID", as_index=False)["评分"].mean()
        sheet3 = reg.merge(avg_score, on="千川素材ID", how="left")
        sheet3["评分"] = sheet3["评分"].fillna(0.0)
        # ✅ 使用 list 选择列，且缺列兜底
        final_cols = ["千川素材ID","编导","拍摄","剪辑","评分"]
        for c in final_cols:
            if c not in sheet3.columns:
                sheet3[c] = "" if c != "评分" else 0.0
        sheet3 = sheet3[final_cols].sort_values(["评分"], ascending=False).reset_index(drop=True)

        info("══ 导出 Excel ──")
        with pd.ExcelWriter(OUT_XLSX, engine="openpyxl") as w:
            sheet1.to_excel(w, sheet_name="Sheet1_日汇总", index=False)
            sheet2.to_excel(w, sheet_name="Sheet2_素材评分明细", index=False)
            sheet3.to_excel(w, sheet_name="Sheet3_千川素材ID登记表", index=False)
        info(f"✅ 已输出：{OUT_XLSX}")
        info(f"📄 运行日志：{LOG_FILE}")
    except Exception as e:
        info("❌ 发生异常：")
        traceback.print_exc(file=sys.stdout)
        info(f"📄 运行日志：{LOG_FILE}")
        raise

if __name__ == "__main__":
    main()
