from ucimlrepo import fetch_ucirepo
import pandas as pd

# carga  el dataset Wine Quality
wine = fetch_ucirepo(id=109)

# Unir features y target
df = pd.concat([wine.data.features, wine.data.targets], axis=1)

# Guardar como CSV
df.to_csv("winequality.csv", index=False)

print("Dataset guardado como winequality.csv")
