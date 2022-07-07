import pandas as pd

COLUMN_INFO_MOCK = pd.DataFrame({
    'column_name': ['testcol1','testcol2','testcol3'],
    'cardinality': [4, 4, 20],
    'dtype': ['int64', 'object','int64'],
    'cardinality_rating': ['LOW','LOW', 'HIGH'],
    'variable_type': ['Numeric', 'Categorical', 'Numeric'],
    'cardinality_pct': [0.2, 0.2, 1.0],
    'nulls': [0, 2, 0],
    'null_pct': [0.0, 0.1, 0.0]
})