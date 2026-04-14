import Haver
import pandas as pd

Haver.direct(1)
try:
    print("Trying Haver.dbcodes('USECON', format='full')")
    codes = Haver.dbcodes('USECON', format='full')
    print("Type:", type(codes))
    if isinstance(codes, pd.DataFrame):
        print(codes.head())
    else:
        print(codes[:10])
except Exception as e:
    print(f"Error: {e}")
