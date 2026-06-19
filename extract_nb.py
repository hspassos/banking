import json
with open('/home/hspassos/mestrado/banking/modelo_estrutural.ipynb', encoding='utf-8') as f:
    nb = json.load(f)
with open('/home/hspassos/mestrado/banking/code_extract.py', 'w', encoding='utf-8') as f:
    for c in nb.get('cells', []):
        if c.get('cell_type') == 'code':
            f.write(''.join(c['source']) + '\n#-------\n')
