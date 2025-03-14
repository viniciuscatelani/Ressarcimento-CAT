# tabela_2 = tabela_2.drop('Unnamed: 0', axis=1)
df_final = df_final.sort_values(by=['COD_ITEM', 'DATA', 'IND_OPER', 'SUB_TIPO'], 
                            ascending=[True, True, True, True]).reset_index().drop('index', axis=1)


df_final['QTD_CAT'] = df_final['QTD_CAT'].astype(float)

ficha_3_1 = df_final[['CHV_DOC', 'DATA', 'CFOP', 'NUM_ITEM', 'COD_ITEM', 'IND_OPER', 'SUB_TIPO', 'QTD_CAT', 'Valor ICMS Operação',
                        'CST', 'ALIQUOTA', 'FONTE']]

query = 'SELECT * FROM produtos'
produtos = pd.read_sql_query(query, connection)

# Ordenação da tabela de acordo com os critérios definidos
ficha_3_1['DATA'] = pd.to_datetime(ficha_3_1['DATA'], format='%Y-%m-%d')
ficha_3_1 = ficha_3_1.sort_values(by=['COD_ITEM', 'DATA', 'IND_OPER', 'SUB_TIPO'], 
                            ascending=[True, True, True, True])

ficha_3_1['ICMS_TOT'] = df_final['ICMS_TOT'].astype(float)
ficha_3_1['VALOR'] = df_final['VALOR']

ficha_3_1 = ficha_3_1.merge(produtos[['codigo_produto', 'mva_antes']], 
                    left_on='COD_ITEM',
                    right_on='codigo_produto',
                    how='left')
ficha_3_1 = ficha_3_1.drop('codigo_produto', axis=1).rename(columns={'mva_antes': 'MVA'})

ficha_3_1['VALOR'] = ficha_3_1['VALOR'].astype(str).replace('-', np.nan).astype(float)
ficha_3_1['ALIQUOTA'] = ficha_3_1['ALIQUOTA'].astype(str).replace('-', np.nan).astype(float)
ficha_3_1['MVA'] = ficha_3_1['MVA'].astype(str).replace('-', np.nan).replace('None', np.nan).astype(float)


ficha_3_1['ICMS_TOT'] = np.where((ficha_3_1['CST'].astype(float) == 60) & (ficha_3_1['ICMS_TOT'] == 0), 
                            ficha_3_1['VALOR'] * (ficha_3_1['ALIQUOTA'] / 100) * ficha_3_1['MVA'] * 0.8,
                            ficha_3_1['ICMS_TOT'])

ficha_3_1['ICMS_TOT'] = ficha_3_1['ICMS_TOT'].fillna(0)

ficha_3_1['Valor ICMS Operação'] = np.where((ficha_3_1['CFOP'].astype(float) == 1403) & (ficha_3_1['Valor ICMS Operação'].astype(float) == 0),
                                        (ficha_3_1['ALIQUOTA'].astype(float)/ 100) * ficha_3_1['VALOR'].astype(float),
                                        ficha_3_1['Valor ICMS Operação'])

data = ficha_3_1[['COD_ITEM', 'DATA', 'QTD_CAT', 'IND_OPER']]

# Transformação da coluna 'DATA' para o tipo correto
data['DATA'] = pd.to_datetime(data['DATA'], format='%Y-%m-%d')

# Ordenação do dataset para cálculo correto do ICMS inicial

data['IND_OPER'] = data['IND_OPER'].astype(int)
data = data.sort_values(by=['COD_ITEM','DATA', 'IND_OPER'], ascending=[True, True, True])

# Cáculo dos saldos das operações para definição da Quantidade Inicial
data['OPER'] = np.where(data['IND_OPER'] == 0, data['QTD_CAT'], -data['QTD_CAT'])
data['SALDO'] = data.groupby('COD_ITEM')['OPER'].cumsum()

# Criação da coluna para as informações da Quantidade Inicial

min_saldo = data.groupby('COD_ITEM')['SALDO'].transform('min')

# Set 'QTD_INI' to the minimum 'SALDO' value if it's less than or equal to 0,
# otherwise set it to 0
data['QTD_INI'] = np.where(min_saldo <= 0, min_saldo.abs(), 0)

# Get the first rows within each group
first_rows = data.groupby('COD_ITEM').head(1)

# Set 'QTD_INI' to NaN for all rows except the last rows within each group
data['QTD_INI'] = np.where(data.index.isin(first_rows.index), data['QTD_INI'], np.nan)

# Update 'QTD_INI' with calculated values for the last rows
data.loc[first_rows.index, 'QTD_INI'] = first_rows['QTD_INI']

# Cálculo do ICMS inicial 

data['SUB_TIPO'] = df_final['SUB_TIPO']
data['ICMS_TOT_0'] = ficha_3_1['ICMS_TOT']
data['CST'] = ficha_3_1['CST']
data['Valor ICMS Operação'] = ficha_3_1['Valor ICMS Operação']
data['VALOR_UNIT'] = np.where((data['SUB_TIPO'] == 1) & (data['IND_OPER'] == 0),
                                data['ICMS_TOT_0'].fillna(0) / data['QTD_CAT'],
                                np.nan)
data['VALOR_OP_UNIT'] = np.where((data['SUB_TIPO'] == 1) & (data['IND_OPER'] == 0),
                                data['Valor ICMS Operação'].fillna(0) / data['QTD_CAT'],
                                np.nan)

grouped_data = data.groupby('COD_ITEM').agg({
    'VALOR_UNIT': 'mean',
    'VALOR_OP_UNIT': 'mean',
    'QTD_INI': 'first'
})

# Cálculo vetorizado para icms_init
grouped_data['ICMS_INI'] = grouped_data['VALOR_UNIT'] * (1 - 0.3 * 0.7) * grouped_data['QTD_INI']
grouped_data['ICMS_OP_INI'] = grouped_data['VALOR_OP_UNIT'] * (1 - 0.3 * 0.7) * grouped_data['QTD_INI']
data = data.merge(grouped_data[['ICMS_INI', 'ICMS_OP_INI']], how='left', left_on='COD_ITEM', right_index=True)

mask = data.duplicated(subset='COD_ITEM', keep='first')

# Definindo como nulo valores de 'ICMS_INI' onde a máscara é True, indicando que não é a primeira linha para 'COD_ITEM'
data['ICMS_INI'] = np.where(mask, np.nan, data['ICMS_INI'])
data['ICMS_OP_INI'] = np.where(mask, np.nan, data['ICMS_OP_INI'])

produtos_somente_saida = df_final.groupby('COD_ITEM')['IND_OPER'].all()
produtos_somente_saida = produtos_somente_saida[produtos_somente_saida == True].index
prods_icms_0 = data[(data['QTD_INI'] != 0) & ((data['ICMS_INI'] == 0) | (data['ICMS_OP_INI'] == 0))]
for produto in list(produtos_somente_saida) + list(prods_icms_0['COD_ITEM'].unique()) + list(prods_icms_0['COD_ITEM'].unique()):
# for produto in data['COD_ITEM'].unique():
# Executa a consulta para obter os valores mva_antes e mva_depois
    query = f"SELECT mva_antes, mva_depois FROM produtos WHERE codigo_produto = '{produto}'"
    try:
        mva = float(pd.read_sql_query(query, connection).dropna(axis=1).iloc[0,0])
    except:
        mva = 0
    
    # Calcula o lucro e a quantidade total
    lucro = sum(df_final[(df_final['COD_ITEM'] == produto) & (df_final['IND_OPER'] == 1)]['VALOR'])
    tot_qtde = sum(df_final[(df_final['COD_ITEM'] == produto) & (df_final['IND_OPER'] == 1)]['QTD_CAT'])
    aliquota = df_final[df_final['COD_ITEM'] == produto]['ALIQUOTA'].values[0] / 100

    
    try:
        # Calcula o valor unitário, base ST estimada e ICMS
        valor_unit = (lucro * 0.8) / tot_qtde
        base_st_estimada = valor_unit + (valor_unit * mva)
        icms_op_unit = valor_unit * aliquota
        icms_suport_unit = base_st_estimada * aliquota
        icms_inicial = icms_suport_unit * tot_qtde

    except:
        icms_inicial = 0
        icms_op_unit = 0
        

    # Verifica se o produto está presente no DataFrame 'data'
    produto_idx = data[data['COD_ITEM'] == produto].index

    if not produto_idx.empty:
        # Garante que a coluna 'ICMS_INI' seja do tipo float
        data['ICMS_INI'] = data['ICMS_INI'].astype(float)
        data['ICMS_OP_INI'] = data['ICMS_OP_INI'].astype(float)

        # Atualiza o valor na primeira linha correspondente ao COD_ITEM
        data.loc[produto_idx[0], 'ICMS_INI'] = icms_inicial
        data.loc[produto_idx[0], 'ICMS_OP_INI'] = icms_op_unit * data.loc[produto_idx[0], 'QTD_INI']

# Definição das colunas para cálculo de ressarcimento de acordo com as regras
ficha_3_1['QTD_INI'] = data['QTD_INI'].astype(float)
ficha_3_1['ICMS_INI'] = data['ICMS_INI'].astype(float)
ficha_3_1['ICMS_OP_INI'] = data['ICMS_OP_INI'].astype(float)
ficha_3_1['ICMS_INI'] = np.where((ficha_3_1['QTD_INI'] == 0) | (ficha_3_1['ICMS_INI'].isnull() & ficha_3_1['QTD_INI'].notnull()),
                                0,
                                ficha_3_1['ICMS_INI'])

ficha_3_1 = ficha_3_1[['CHV_DOC', 'DATA', 'CFOP', 'NUM_ITEM', 'COD_ITEM', 'IND_OPER', 
                    'SUB_TIPO', 'QTD_CAT', 'Valor ICMS Operação','CST', 'QTD_INI','ICMS_INI', 'ICMS_OP_INI',
                    'ALIQUOTA', 'FONTE', 'ICMS_TOT', 'VALOR']]

# Inicializa as colunas
ficha_3_1['valor_op_fixo'] = 0
ficha_3_1['qtd_fixa'] = 0

# Processamento dos valores fixos acumulados
for cod in ficha_3_1['COD_ITEM'].unique():
    subset = ficha_3_1[ficha_3_1['COD_ITEM'] == cod].copy()

    acum_val = 0
    acum_qtd = 0

    for i, row in subset.iterrows():
        if row['IND_OPER'] == 0:
            ficha_3_1.loc[i, 'valor_op_fixo'] = acum_val
            ficha_3_1.loc[i, 'qtd_fixa'] = acum_qtd

            acum_val += row['Valor ICMS Operação']
            acum_qtd += row['QTD_CAT']

# Cálculo do valor médio
valor_medio = []

for cod in ficha_3_1['COD_ITEM'].unique():
    subset = ficha_3_1[ficha_3_1['COD_ITEM'] == cod].copy().reset_index()

    for i, row in subset.iterrows():
        if i == 0:  # Primeira linha
            if row['IND_OPER'] == 0:
                # Calcular com valores acumulados
                num = row['valor_op_fixo'] + row['Valor ICMS Operação'] + row['ICMS_OP_INI']
                den = row['qtd_fixa'] + row['QTD_CAT'] + row['QTD_INI']
                valor_medio.append(num / den)
            elif row['IND_OPER'] == 1:
                # Calcular com ICMS_OP_INI e QTD_INI
                num = row['ICMS_OP_INI']
                den = row['QTD_INI']
                valor_medio.append(num / den)
        else:  # Demais linhas
            if row['IND_OPER'] == 0:
                # Calcular com valores acumulados
                num = row['valor_op_fixo'] + row['Valor ICMS Operação'] + subset.iloc[0]['ICMS_OP_INI']
                den = row['qtd_fixa'] + row['QTD_CAT'] + subset.iloc[0]['QTD_INI']
                valor_medio.append(num / den)
            elif row['IND_OPER'] == 1:
                # Para IND_OPER == 1, deixamos como NaN
                valor_medio.append(np.nan)

# Adiciona a coluna 'valor_medio' ao DataFrame e preenche valores ausentes
ficha_3_1['valor_medio'] = valor_medio
ficha_3_1['valor_medio'] = ficha_3_1['valor_medio'].ffill()  # Preenche valores ausentes com o valor anterior

# Cálculos finais
ficha_3_1['COD_LEGAL'] = df_final['COD_LEGAL'].astype(float)
ficha_3_1['ICMS_EFETIVO_ENTRADA'] = ficha_3_1['valor_medio'] * ficha_3_1['QTD_CAT']
ficha_3_1['ICMS_EFETIVO_ENTRADA'] = np.where(ficha_3_1['COD_LEGAL'] > 1,
                                        ficha_3_1['ICMS_EFETIVO_ENTRADA'],
                                        np.nan)

ficha_3_1['IND_OPER'] = ficha_3_1['IND_OPER'].astype(int)
ficha_3_1['SUB_TIPO'] = ficha_3_1['SUB_TIPO'].astype(int)

mask_condition1 = (ficha_3_1['IND_OPER'] == 0) & (ficha_3_1['SUB_TIPO'] == 1)
mask_condition2 = (ficha_3_1['IND_OPER'] != 0) & (ficha_3_1['SUB_TIPO'] == -1)

qtd_ent1_devolv_ent = list(np.where(mask_condition1, ficha_3_1['QTD_CAT'], 0) + np.where(mask_condition2, -ficha_3_1['QTD_CAT'], 0))
ficha_3_1['QTD_ent1_devolv_ent'] = qtd_ent1_devolv_ent

mask = ficha_3_1['QTD_ent1_devolv_ent'] > 0

ficha_3_1['ICMS_TOT_ent_unit'] = np.where(mask, ficha_3_1['ICMS_TOT'] / ficha_3_1['QTD_ent1_devolv_ent'], 0)

ficha_3_1['ULT_ICMS_TOT_ent_unit'] = np.nan

values = np.where(
    (ficha_3_1['QTD_INI'] != 0) & 
    (ficha_3_1['COD_ITEM'] == ficha_3_1['COD_ITEM'].shift()) & 
    (ficha_3_1['QTD_ent1_devolv_ent'].shift() > 0),
    ficha_3_1['ICMS_TOT_ent_unit'].shift(),
    np.where(
        (ficha_3_1['QTD_INI'] == 0),
        0.01,
        ficha_3_1['ICMS_INI'] / ficha_3_1['QTD_INI']
    )
)


ficha_3_1['ULT_ICMS_TOT_ent_unit'] = values

ficha_3_1['ULT_ICMS_TOT_ent_unit'] = ficha_3_1['ULT_ICMS_TOT_ent_unit'].ffill()

mask_condition1 = (ficha_3_1['IND_OPER'] == 1) & (ficha_3_1['SUB_TIPO'] == 1)
mask_condition2 = (ficha_3_1['IND_OPER'] != 1) & (ficha_3_1['SUB_TIPO'] == -1)

qtd_saida_1_devolv_saida_t = np.where(mask_condition1, ficha_3_1['QTD_CAT'], 0) + np.where(mask_condition2, -ficha_3_1['QTD_CAT'], 0)

ficha_3_1['qtd_saida_1_devolv_saida'] = qtd_saida_1_devolv_saida_t

ficha_3_1['QTD_SALDO'] = ficha_3_1.groupby('COD_ITEM').apply(lambda x: (
x['QTD_INI'].iloc[0] + (x['QTD_ent1_devolv_ent'] - x['qtd_saida_1_devolv_saida']).cumsum())).reset_index(level=0, drop=True)

ficha_3_1['QTD_SALDO'] = np.where((ficha_3_1['QTD_SALDO'] < 0.01) & (ficha_3_1['QTD_SALDO'] > -0.01),
                                        0,
                                        ficha_3_1['QTD_SALDO'])      

ficha_3_1['DATA'] = ficha_3_1['DATA'].astype(str)
ficha_3_1['REF'] = ficha_3_1['DATA']
ficha_3_1['REF'] = [datetime.strptime(x, '%Y-%m-%d').strftime('%m-%Y') for x in ficha_3_1['REF']]

# Definição do primeiro valor de ICMS_SAIDA_UNI
icms_saida_uni = [] 

# Definição do primeiro valor de ULT_ICMS_SAIDA_UNI
ult_icms_saida_uni = [] 

# Definição do primeiro valor de ICMS_SAIDA
icms_saida = [] 

# Definição do primeiro valor de ICMS_TOT_SALDO
icms_tot_saldo = [] 

# Definição do primeiro valor de ICMS_TOT_1
icms_tot_1 = []

# Definição de todos os outros valores para cada campo acima
for code in ficha_3_1['COD_ITEM'].unique():

    icms_saida_uni_int = []
    ult_icms_saida_uni_int = []
    icms_saida_int = []
    icms_tot_saldo_int = []
    icms_tot_1_int = []
    qtd_saida_1_devolv_saida = list(ficha_3_1[ficha_3_1['COD_ITEM'] == code]['qtd_saida_1_devolv_saida'])
    qtd_saldo = list(ficha_3_1[ficha_3_1['COD_ITEM'] == code]['QTD_SALDO'])
    qtd_cat = list(ficha_3_1[ficha_3_1['COD_ITEM'] == code]['QTD_CAT'])
    ref = list(ficha_3_1[ficha_3_1['COD_ITEM'] == code]['REF'])

    for i,row in ficha_3_1[ficha_3_1['COD_ITEM']==code].reset_index(drop=True).iterrows():

    # ICMS_SAIDA_UNI

        if i == 0:
            if qtd_saida_1_devolv_saida[0] > 0:
                if row['QTD_INI'] == 0:
                    icms_saida_uni_int.append(0)
                else:  
                    icms_saida_uni_int.append(row['ICMS_INI'] / row['QTD_INI'])
            else:
                icms_saida_uni_int.append(0)

        else:
            if (qtd_saida_1_devolv_saida[i] != 0) and (qtd_saldo[i-1] != 0):  
                icms_saida_uni_int.append(icms_tot_saldo_int[i-1] / qtd_saldo[i-1])               
            else:      
                icms_saida_uni_int.append(0)

    # ULT_ICMS_SAIDA_UNI
        if i== 0:
            if row['QTD_INI'] == 0:
                ult_icms_saida_uni_int.append(0.01)
            else:           
                ult_icms_saida_uni_int.append(
                    row['ICMS_INI'] / row['QTD_INI'] if row['QTD_INI'] != 0 else float("nan"))
        else:
            if qtd_saida_1_devolv_saida[i-1] > 0:
                ult_icms_saida_uni_int.append(icms_saida_uni_int[i-1])
            else:
                ult_icms_saida_uni_int.append(ult_icms_saida_uni_int[i-1])


    # ICMS_SAIDA
        if qtd_saida_1_devolv_saida[i] < 0:
            icms_saida_int.append(qtd_saida_1_devolv_saida[i]*ult_icms_saida_uni_int[i])
        else:
            icms_saida_int.append(icms_saida_uni_int[i]*qtd_saida_1_devolv_saida[i])

    # ICMS_TOT_1
        if i == 0:
            if (row['QTD_ent1_devolv_ent'] < 0) & (row['QTD_CAT'] == row['QTD_INI']) :
                icms_tot_1_int.append(-row['ICMS_INI'])
            else:
                icms_tot_1_int.append(row['ICMS_TOT'])
        else:
            if row['QTD_ent1_devolv_ent'] < 0:
                if (qtd_cat[i] == qtd_saldo[i-1]):
                    icms_tot_1_int.append(-1*icms_tot_saldo_int[i-1])
                else:
                    icms_tot_1_int.append(max(row['QTD_ent1_devolv_ent'] * row['ULT_ICMS_TOT_ent_unit'], -1*icms_tot_saldo_int[i-1]))
            else:
                icms_tot_1_int.append(row['ICMS_TOT']) 

    # ICMS_TOT_SALDO
        if i == 0:
            icms_tot_saldo_int.append(row['ICMS_INI'] + icms_tot_1_int[i] - icms_saida_int[i])
        else:
            icms_tot_saldo_int.append(icms_tot_saldo_int[i-1] + icms_tot_1_int[i] - icms_saida_int[i])


    icms_saida_uni += icms_saida_uni_int
    ult_icms_saida_uni += ult_icms_saida_uni_int
    icms_saida += icms_saida_int
    icms_tot_saldo += icms_tot_saldo_int
    icms_tot_1 += icms_tot_1_int

ficha_3_1['ICMS_SAIDA_UNI'] = icms_saida_uni
ficha_3_1['ULT_ICMS_SAIDA_UNI'] = ult_icms_saida_uni
ficha_3_1['ICMS_SAIDA'] = icms_saida
ficha_3_1['ICMS_TOT_SALDO'] = icms_tot_saldo
ficha_3_1['ICMS_TOT_SALDO'] = np.where((ficha_3_1['ICMS_TOT_SALDO'] < 0.01) & (ficha_3_1['ICMS_TOT_SALDO'] > -0.01),
                                        0,
                                        ficha_3_1['ICMS_TOT_SALDO'])
ficha_3_1['ICMS_TOT_1'] = icms_tot_1

ficha_3_1['ICMS_TOT_PCAT'] = np.where((ficha_3_1['qtd_saida_1_devolv_saida'] < 0),
                                    np.abs(ficha_3_1['ICMS_SAIDA_UNI']),
                                    np.where((ficha_3_1['QTD_ent1_devolv_ent'] != 0),
                                            np.abs(ficha_3_1['ICMS_TOT_1']),
                                            np.nan))

if ficha_3_1[ficha_3_1['ICMS_TOT_PCAT'] < 0].shape[0] > 0:
    mensagem = 'EXISTE ICMS TOT PCAT NEGATIVO. FAVOR CHECAR!!'
    print(mensagem)

ficha_3_1['VLR_CONF_0'] = df_final['VL_CONFR_0'].astype(float)

mask = ficha_3_1['qtd_saida_1_devolv_saida'] > 0
vlr_confr_unit = np.where(mask, ficha_3_1['VLR_CONF_0']/ficha_3_1['qtd_saida_1_devolv_saida'], np.nan)

ficha_3_1['VLR_CONFR_UNIT'] = vlr_confr_unit

ult_vlr_confr_unit = [0]
for i in range(1, ficha_3_1.shape[0]):
    if ficha_3_1.iloc[i]['COD_ITEM'] != ficha_3_1.iloc[i-1]['COD_ITEM']:
        ult_vlr_confr_unit.append(0)
    else:
        if math.isnan(ficha_3_1.iloc[i-1]['VLR_CONFR_UNIT']):
            ult_vlr_confr_unit.append(ult_vlr_confr_unit[i-1])
        else:
            ult_vlr_confr_unit.append(ficha_3_1.iloc[i-1]['VLR_CONFR_UNIT'])

ficha_3_1['ULT_VLR_CONFR_UNIT'] = ult_vlr_confr_unit

ficha_3_1['VLR_CONFR_1'] = np.where(ficha_3_1['qtd_saida_1_devolv_saida'] > 0,
                                ficha_3_1['VLR_CONF_0'].astype(float),
                                np.where(ficha_3_1['qtd_saida_1_devolv_saida'] < 0,
                                        ficha_3_1['ULT_VLR_CONFR_UNIT'] * ficha_3_1['qtd_saida_1_devolv_saida'],
                                        0))

ficha_3_1['VLR_CONFR_1'] = np.where(ficha_3_1['VLR_CONFR_1'] <= 0.01,
                                            0.01,
                                            ficha_3_1['VLR_CONFR_1'])
                                            
ficha_3_1['VLR_CONFR_1'] = np.where(ficha_3_1['IND_OPER'] == 1, ficha_3_1['VLR_CONFR_1'], np.nan)

mask_cod_1 = np.abs(ficha_3_1['ICMS_SAIDA']) > np.abs(ficha_3_1['VLR_CONFR_1'])
mask_cod_3 = ficha_3_1['VLR_CONFR_1'] < ficha_3_1['ICMS_EFETIVO_ENTRADA']
apenas_movimentacao = [1102, 2102, 2209, 2949, 5101, 5102, 
                        5118, 5152, 5201, 5202, 5210,
                        5401, 5409, 5410, 5411, 5413, 5551, 5556, 5603,
                        5910, 5911, 5916, 5920, 5921, 5922, 5923, 5949, 
                        6101, 6103, 6119, 6202, 6401, 6403, 6411,
                        6556, 6910, 6911, 6922, 6923]

ficha_3_1['VLR_RESSARCIMENTO'] = 0

ficha_3_1['VLR_RESSARCIMENTO'] = np.where(ficha_3_1['CFOP'].isin(apenas_movimentacao), 0, ficha_3_1['VLR_RESSARCIMENTO'])

ficha_3_1['VLR_RESSARCIMENTO'] = np.where((ficha_3_1['CFOP'].isin([2202, 5117, 5120, 5929])) & (ficha_3_1['CST'].astype(float) != 60), 
                                        0, 
                                        ficha_3_1['VLR_RESSARCIMENTO'])

ficha_3_1['VLR_RESSARCIMENTO'] = np.where((ficha_3_1['CFOP'].isin([2202, 5117, 5120, 5929])) & (ficha_3_1['CST'].astype(float) == 60) & mask_cod_1,
                                        ficha_3_1['ICMS_SAIDA'] - ficha_3_1['VLR_CONFR_1'], 
                                        ficha_3_1['VLR_RESSARCIMENTO'])

ficha_3_1['VLR_RESSARCIMENTO'] = np.where((ficha_3_1['COD_LEGAL'] == 1) & mask_cod_1 & (~ficha_3_1['CFOP'].isin(apenas_movimentacao)) & (~ficha_3_1['CFOP'].isin([2202, 5117, 5120, 5929])),
                                        ficha_3_1['ICMS_SAIDA'] - ficha_3_1['VLR_CONFR_1'], 
                                        ficha_3_1['VLR_RESSARCIMENTO'])

ficha_3_1['VLR_RESSARCIMENTO'] = np.where((ficha_3_1['COD_LEGAL'] == 2) & (~ficha_3_1['CFOP'].isin(apenas_movimentacao)) & (~ficha_3_1['CFOP'].isin([2202, 5117, 5120, 5929])), 
                                        ficha_3_1['ICMS_EFETIVO_ENTRADA'].fillna(0), 
                                        ficha_3_1['VLR_RESSARCIMENTO'])

ficha_3_1['VLR_RESSARCIMENTO'] = np.where((ficha_3_1['COD_LEGAL'] == 3) & mask_cod_3 & (~ficha_3_1['CFOP'].isin(apenas_movimentacao)) & (~ficha_3_1['CFOP'].isin([2202, 5117, 5120, 5929])), 
                                        ficha_3_1['ICMS_EFETIVO_ENTRADA'].fillna(0), 
                                        ficha_3_1['VLR_RESSARCIMENTO'])

ficha_3_1['VLR_RESSARCIMENTO'] = np.where((ficha_3_1['COD_LEGAL'] == 4) & (~ficha_3_1['CFOP'].isin(apenas_movimentacao)) & (~ficha_3_1['CFOP'].isin([2202, 5117, 5120, 5929])), 
                                        np.abs(ficha_3_1['ICMS_SAIDA'] - ficha_3_1['ICMS_EFETIVO_ENTRADA'].fillna(0)), 
                                        ficha_3_1['VLR_RESSARCIMENTO'])

# Garantir que o cálculo de 'VLR_RESSARCIMENTO' seja 0 para as condições específicas
ficha_3_1['VLR_RESSARCIMENTO'] = np.where(
    (ficha_3_1['CFOP'].isin([2202, 5117, 5120, 5929])) & (ficha_3_1['CST'].astype(float) != 60),
    0,
    ficha_3_1['VLR_RESSARCIMENTO']
)

mask_cod_1 = np.abs(ficha_3_1['ICMS_SAIDA']) < np.abs(ficha_3_1['VLR_CONFR_1'])
mask_cod_3 = ficha_3_1['VLR_CONFR_1'] > ficha_3_1['ICMS_EFETIVO_ENTRADA']

ficha_3_1['VLR_COMPLEMENTO'] = 0

ficha_3_1['VLR_COMPLEMENTO'] = np.where(ficha_3_1['CFOP'].isin(apenas_movimentacao), 
                                    0, 
                                    ficha_3_1['VLR_COMPLEMENTO'])

ficha_3_1['VLR_COMPLEMENTO'] = np.where((ficha_3_1['CFOP'].isin([2202, 5117, 5120, 5929])) & (ficha_3_1['CST'].astype(float) != 60), 
                                        0, 
                                        ficha_3_1['VLR_COMPLEMENTO'])

ficha_3_1['VLR_COMPLEMENTO'] = np.where((ficha_3_1['CFOP'].isin([2202, 5117, 5120, 5929])) & (ficha_3_1['CST'].astype(float) == 60) & mask_cod_1,
                                        -ficha_3_1['ICMS_SAIDA'] + ficha_3_1['VLR_CONFR_1'], 
                                        ficha_3_1['VLR_COMPLEMENTO'])

ficha_3_1['VLR_COMPLEMENTO'] = np.where((ficha_3_1['COD_LEGAL'] == 1) & mask_cod_1 & (~ficha_3_1['CFOP'].isin(apenas_movimentacao)) & (~ficha_3_1['CFOP'].isin([2202, 5117, 5120, 5929])),
                                        -ficha_3_1['ICMS_SAIDA'] + ficha_3_1['VLR_CONFR_1'], 
                                        ficha_3_1['VLR_COMPLEMENTO'])

ficha_3_1['VLR_COMPLEMENTO'] = np.where((ficha_3_1['COD_LEGAL'] == 2) & (~ficha_3_1['CFOP'].isin(apenas_movimentacao)) & (~ficha_3_1['CFOP'].isin([2202, 5117, 5120, 5929])), 
                                        0, 
                                        ficha_3_1['VLR_COMPLEMENTO'])

ficha_3_1['VLR_COMPLEMENTO'] = np.where((ficha_3_1['COD_LEGAL'] == 3) & mask_cod_3 & (~ficha_3_1['CFOP'].isin(apenas_movimentacao)) & (~ficha_3_1['CFOP'].isin([2202, 5117, 5120, 5929])), 
                                        ficha_3_1['VLR_CONFR_1'] - ficha_3_1['ICMS_EFETIVO_ENTRADA'], 
                                        ficha_3_1['VLR_COMPLEMENTO'])

ficha_3_1['VLR_COMPLEMENTO'] = np.where((ficha_3_1['COD_LEGAL'] == 4) & (~ficha_3_1['CFOP'].isin(apenas_movimentacao)) & (~ficha_3_1['CFOP'].isin([2202, 5117, 5120, 5929])), 
                                        0, 
                                        ficha_3_1['VLR_COMPLEMENTO'])

# Garantir que o cálculo de 'VLR_COMPLEMENTO' seja 0 para as condições específicas
ficha_3_1['VLR_COMPLEMENTO'] = np.where(
    (ficha_3_1['CFOP'].isin([2202, 5117, 5120, 5929])) & (ficha_3_1['CST'].astype(float) != 60),
    0,
    ficha_3_1['VLR_COMPLEMENTO']
)

ficha_3_1['COD_LEGAL'] = np.where((ficha_3_1['CFOP'].isin(apenas_movimentacao)) | ((ficha_3_1['CFOP'].isin([2202, 5117, 5120, 5929])) & (ficha_3_1['CST'].astype(float) != 60)),
                                0,
                                ficha_3_1['COD_LEGAL'])

ficha_3_1['COD_LEGAL'] = np.where((ficha_3_1['COD_LEGAL'] == 1) & (ficha_3_1['VLR_RESSARCIMENTO'] != 0) & (ficha_3_1['ALIQUOTA'] != 0), 1, ficha_3_1['COD_LEGAL'])
ficha_3_1['COD_LEGAL'] = np.where(ficha_3_1['CFOP'].isin([5404, 5403, 5401]), 1, ficha_3_1['COD_LEGAL'])
ficha_3_1['COD_LEGAL'] = np.where(ficha_3_1['CFOP'].isin([1411, 5409]), 0, ficha_3_1['COD_LEGAL'])
ficha_3_1['COD_LEGAL'] = np.where(ficha_3_1['CFOP'].isin([1403, 1409, 5411]), np.nan, ficha_3_1['COD_LEGAL'])
# ficha_3_1.loc[pd.isna(ficha_3_1['COD_LEGAL']), 'COD_LEGAL_PCAT'] = np.nan

ficha_3_1['VLR_CONFR_PCAT'] = np.where(ficha_3_1['COD_LEGAL'].isin([1, 3]), 
                                        np.abs(ficha_3_1['VLR_CONFR_1']), np.nan)

ficha_3_1['VLR_CONFR_PCAT'] = np.where((ficha_3_1['CFOP'].isin(apenas_movimentacao)) | ((ficha_3_1['CFOP'].astype(int).isin([2202, 5117, 5120, 5929])) & (ficha_3_1['CST'].astype(float) != 60)), 
                                        np.nan, np.abs(ficha_3_1['VLR_CONFR_PCAT']))

if ficha_3_1[(ficha_3_1['COD_LEGAL'].isin([1, 3])) & (ficha_3_1['VLR_CONFR_PCAT'] <= 0)].shape[0] > 0:
    mensagem = 'Há valores de confronto inconsistentes. Favor checar.'
    print(mensagem)
    sys.exit()

mask = (ficha_3_1['COD_ITEM'].shift(-1) != ficha_3_1['COD_ITEM']) | (ficha_3_1['REF'].shift(-1) != ficha_3_1['REF'])
saldo_final_mes_qtd = np.where(mask, ficha_3_1['QTD_SALDO'], np.nan)

ficha_3_1['SALDO_FINAL_MES_QTD'] = saldo_final_mes_qtd

mask = (ficha_3_1['COD_ITEM'].shift(-1) != ficha_3_1['COD_ITEM']) | (ficha_3_1['REF'].shift(-1) != ficha_3_1['REF'])
saldo_final_mes_icms = np.where(mask, ficha_3_1['ICMS_TOT_SALDO'], np.nan)

ficha_3_1['SALDO_FINAL_MES_ICMS'] = saldo_final_mes_icms

ficha_3_1['Valor Base Cálculo ICMS ST Retido Operação Anterior'] = df_final['Valor Base Cálculo ICMS ST Retido Operação Anterior']
ficha_3_1['Valor Complementar'] = df_final['Valor Complementar']
ficha_3_1['vBCST'] = df_final['vBCST']
ficha_3_1['Valor ICMS Substituição Tributária'] = df_final['Valor ICMS Substituição Tributária']
ficha_3_1['Valor ICMS Operação'] = df_final['Valor ICMS Operação']
ficha_3_1['CNPJ EMITENTE'] = df_final['CNPJ EMITENTE']

ficha_3_1 = ficha_3_1[['CHV_DOC', 'DATA', 'CFOP', 'NUM_ITEM', 'COD_ITEM', 'IND_OPER', 'SUB_TIPO', 'QTD_CAT',
                    'QTD_INI', 'ICMS_INI', 'QTD_ent1_devolv_ent', 'ICMS_TOT', 'ICMS_TOT_ent_unit',
                    'ULT_ICMS_TOT_ent_unit', 'ICMS_TOT_1', 'qtd_saida_1_devolv_saida', 'ICMS_SAIDA_UNI',
                    'ULT_ICMS_SAIDA_UNI', 'ICMS_SAIDA', 'ICMS_TOT_PCAT', 'VLR_CONF_0', 'VLR_CONFR_UNIT',
                    'ULT_VLR_CONFR_UNIT', 'VLR_CONFR_1', 'QTD_SALDO', 'ICMS_TOT_SALDO', 'VLR_RESSARCIMENTO',
                    'VLR_COMPLEMENTO', 'COD_LEGAL', 'VLR_CONFR_PCAT', 'SALDO_FINAL_MES_QTD',
                    'SALDO_FINAL_MES_ICMS', 'ALIQUOTA', 'VALOR', 'Valor Base Cálculo ICMS ST Retido Operação Anterior',
                    'Valor Complementar', 'Valor ICMS Substituição Tributária', 'Valor ICMS Operação', 'valor_op_fixo',
                    'qtd_fixa', 'valor_medio', 'ICMS_EFETIVO_ENTRADA',
                    'CNPJ EMITENTE', 'vBCST', 'CST',
                    'FONTE']]