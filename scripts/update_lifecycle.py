# scripts/update_lifecycle.py

import yaml
from pathlib import Path
import sys

if len(sys.argv) != 2:
    print("❌ Uso: python scripts/update_lifecycle.py <valore_lifecycle>")
    sys.exit(1)

lifecycle_value = sys.argv[1]

path = Path("catalog-info.yaml")

if not path.exists():
    raise FileNotFoundError("catalog-info.yaml non trovato nella root del repo.")

with path.open("r") as f:
    data = yaml.safe_load(f)

# Modifica il campo
data.setdefault("spec", {})["lifecycle"] = lifecycle_value

# Sovrascrive il file
with path.open("w") as f:
    yaml.dump(data, f, sort_keys=False)

print(f"✅ lifecycle aggiornato a '{lifecycle_value}'")