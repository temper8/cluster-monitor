#!/usr/bin/env python3
import pandas as pd
import io

def parse_sinfo_output(raw_output: str):
    """
    Парсит вывод sinfo -N -l с помощью pandas.read_fwf.
    Возвращает словарь с df, free_nodes, allocated_nodes, down_nodes,
    total_nodes, states, nodes.
    """
    df = pd.read_fwf(io.StringIO(raw_output), header=1)

    # Удаляем звёздочку из состояний (например, down* -> down)
    df['STATE'] = df['STATE'].str.rstrip('*')
    #df['NODES'] = df['NODES'].str.strip()
  
    # Подсчёт узлов по состояниям
    states_counts = df['STATE'].value_counts()
    print(states_counts)

    return {
        "df": df,
        "free_nodes": states_counts['idle'],
        "allocated_nodes": states_counts['allocated'],
        "down_nodes": states_counts['down'],
        "total_nodes": states_counts['idle'] + states_counts['allocated'] + states_counts['down'],
        #"states": states,
        #"nodes": df.to_dict(orient='records')
    }

def count_free_nodes(parsed_data):
    """Возвращает количество свободных узлов."""
    return parsed_data["free_nodes"]

def count_allocated_nodes(parsed_data):
    """Возвращает количество выделенных (allocated) узлов."""
    return parsed_data["allocated_nodes"]

def count_down_nodes(parsed_data):
    """Возвращает количество недоступных (down) узлов."""
    return parsed_data["down_nodes"]