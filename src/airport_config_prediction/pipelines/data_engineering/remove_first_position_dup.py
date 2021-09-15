import pandas as pd

def remove_first_pos_dup(KCLT_firstpos:pd.DataFrame):
    KCLT_firstpos=KCLT_firstpos.sort_values(by=['time_first_tracked'])
    KCLT_firstpos = KCLT_firstpos.drop_duplicates(subset=['gufi'], keep='first')
    return KCLT_firstpos
