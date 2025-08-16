import pandas as pd
import numpy as np

from src.utils.calcular_ressarcimento_v2 import calcular_ressarcimento

def gti_pra_cima(ficha_3, meta_ressarc, top_prods):
    
    '''
    Função para aplicação de GTI para lojas
    com ressarcimento abaixo da porcentagem estabelecida
    como meta
    
    '''

    ressarc = ficha_3['VLR_RESSARCIMENTO'].sum()
    fator = 0.1
    
    # 1ª Iteração
    print('Passando pela primeira iteração do GTI')
    ficha_3['COD_ITEM'] = ficha_3['COD_ITEM'].astype(str)
    pivot_ressarc = ficha_3.pivot_table(index='COD_ITEM', values='VLR_RESSARCIMENTO', aggfunc='sum')
    pivot_ressarc = pivot_ressarc[pivot_ressarc['VLR_RESSARCIMENTO'] > 0]
    pivot_ressarc = pivot_ressarc.sort_values(by='VLR_RESSARCIMENTO', ascending=False)  
    
    incr_max_ref = pivot_ressarc.iloc[top_prods-1].values[0]
    incr_max = pivot_ressarc.iloc[top_prods:]
    incr_max['Diferenças'] = incr_max_ref - incr_max['VLR_RESSARCIMENTO']
    incr_max = incr_max.sort_values(by='COD_ITEM')
    
    qtd_saida = ficha_3[ficha_3['IND_OPER'] == 1].pivot_table(index='COD_ITEM', values='QTD_CAT', aggfunc='sum')
    incr_max_unit = incr_max.merge(qtd_saida, on='COD_ITEM', how='left')
    incr_max_unit['Incremento maximo unitario'] = incr_max_unit['Diferenças'] / incr_max_unit['QTD_CAT']
    incr_max_unit = incr_max_unit.reset_index()
    
    tabela_2_nova = ficha_3.merge(incr_max_unit[['COD_ITEM', 'Incremento maximo unitario']], on='COD_ITEM', how='left')
    tabela_2_nova['Incremento maximo unitario'] = np.where(tabela_2_nova['ICMS_TOT'].fillna(0) == 0,
                                                    0,
                                                    tabela_2_nova['Incremento maximo unitario'])

    tabela_2_nova['Incremento maximo unitario'] = tabela_2_nova['Incremento maximo unitario'].fillna(0)

    tabela_2_nova['ICMS_TOT_FINAL'] = np.where(tabela_2_nova['ICMS_TOT'].fillna(0) != 0,
                                        tabela_2_nova['ICMS_TOT'] * (1 + fator),
                                        0)

    tabela_2_nova['VALOR_ICMS_OP_FINAL'] = np.where(tabela_2_nova['Valor ICMS Operação'].fillna(0) != 0,
                                        tabela_2_nova['Valor ICMS Operação'] * (1 + fator),
                                        0)

    tabela_2_nova['ICMS_TOT_SAIDA_FINAL'] = np.where(tabela_2_nova['ICMS_TOT_SAIDA'].fillna(0) != 0,
                                        tabela_2_nova['ICMS_TOT_SAIDA'] * (1 + fator),
                                        0)

    df_final = ficha_3.copy()
    df_final = df_final.sort_values(by=['COD_ITEM', 'DATA', 'IND_OPER', 'SUB_TIPO'], 
                                    ascending=[True, True, True, True]).reset_index().drop(['index'], axis=1)                                      

    df_final['ICMS_TOT_FINAL'] = tabela_2_nova['ICMS_TOT_FINAL']
    df_final['VALOR_ICMS_OP_FINAL'] = tabela_2_nova['VALOR_ICMS_OP_FINAL']
    df_final['ICMS_TOT_SAIDA_FINAL'] = tabela_2_nova['ICMS_TOT_SAIDA_FINAL']

    df_final.rename(columns={'ICMS_TOT': 'ICMS_TOT_ORIG',
                                'ICMS_TOT_FINAL': 'ICMS_TOT',
                                'ICMS_TOT_SAIDA': 'ICMS_TOT_SAIDA_ORIG',
                                'ICMS_TOT_SAIDA_FINAL': 'ICMS_TOT_SAIDA',
                                'VLR_CONF_0': 'VL_CONFR_0',
                                'Valor ICMS Operação': 'Valor ICMS Operação Orig',
                                'VALOR_ICMS_OP_FINAL': 'Valor ICMS Operação'
                            }, inplace=True)

    df_final = df_final[['CHV_DOC', 'DATA', 'CFOP', 'NUM_ITEM', 'COD_ITEM', 'IND_OPER', 'SUB_TIPO', 'QTD_CAT',
                            'ICMS_TOT','COD_LEGAL', 'VL_CONFR_0', 'ICMS_TOT_SAIDA',
                            'ALIQUOTA', 'VALOR', 'Valor Base Cálculo ICMS ST Retido Operação Anterior',
                            'Valor Complementar', 'Valor ICMS Substituição Tributária', 'Valor ICMS Operação',
                            'CNPJ EMITENTE', 'vBCST', 'CST', 'Valor ICMS Operação Orig', 'ICMS_TOT_ORIG','ICMS_TOT_SAIDA_ORIG',
                            'FONTE']]
    

    
    ficha_3_final = calcular_ressarcimento(df_final)
    
    # 2ª Iteração
    print('Passando pela segunda iteração do GTI')
    ressarc_tot_0_1 = ficha_3_final['VLR_RESSARCIMENTO'].sum()
    delta_fator_0_1 = ressarc_tot_0_1 - ressarc
    meta_delta = meta_ressarc - ressarc
    meta_fator_0_1 = meta_delta/delta_fator_0_1
    meta_fator = meta_fator_0_1*0.1*0.95
    
    ficha_3_final['COD_ITEM'] = ficha_3_final['COD_ITEM'].astype(str)
    pivot_ressarc_final = ficha_3_final.pivot_table(index='COD_ITEM', values='VLR_RESSARCIMENTO', aggfunc='sum')
    pivot_ressarc_final = pivot_ressarc_final[pivot_ressarc_final['VLR_RESSARCIMENTO'] > 0]
    pivot_ressarc_final = pivot_ressarc_final.sort_values(by='VLR_RESSARCIMENTO', ascending=False)
    
    
    incr_max_ref_final = pivot_ressarc_final.iloc[top_prods-1].values[0]
    incr_max_final = pivot_ressarc_final.iloc[top_prods:]
    incr_max_final['Diferenças'] = incr_max_ref_final - incr_max_final['VLR_RESSARCIMENTO']
    incr_max_final = incr_max_final.sort_values(by='COD_ITEM')
    
    qtd_saida_final = ficha_3_final[ficha_3_final['IND_OPER'] == 1].pivot_table(index='COD_ITEM', values='QTD_CAT', aggfunc='sum')
    incr_max_unit_final = incr_max_final.merge(qtd_saida_final, on='COD_ITEM', how='left')
    incr_max_unit_final['Incremento maximo unitario'] = incr_max_unit_final['Diferenças'] / incr_max_unit_final['QTD_CAT']
    
    incr_max_unit_final = incr_max_unit_final.reset_index()
    
    ficha_3_final['ICMS_TOT_ORIG'] = df_final['ICMS_TOT_ORIG']
    ficha_3_final['ICMS_TOT_SAIDA_ORIG'] = df_final['ICMS_TOT_SAIDA_ORIG']
    ficha_3_final = ficha_3_final.merge(incr_max_unit_final[['COD_ITEM', 'Incremento maximo unitario']], on='COD_ITEM', how='left')
    ficha_3_final['Incremento maximo unitario'] = np.where(ficha_3_final['ICMS_TOT_ORIG'].fillna(0) == 0,
                                                 0,
                                                 ficha_3_final['Incremento maximo unitario'])
    ficha_3_final['Incremento maximo unitario'] = ficha_3_final['Incremento maximo unitario'].fillna(0)
    
    ficha_3_final['ICMS_TOT_FINAL'] = np.where(ficha_3_final['ICMS_TOT_ORIG'].fillna(0) != 0,
                                    ficha_3_final['ICMS_TOT_ORIG'] * (1 + meta_fator),
                                    0)

    ficha_3_final['VALOR_ICMS_OP_FINAL'] = np.where(ficha_3_final['Valor ICMS Operação'].fillna(0) != 0,
                                        ficha_3_final['Valor ICMS Operação'] * (1 + meta_fator),
                                        0)

    ficha_3_final['ICMS_TOT_SAIDA_FINAL'] = np.where(ficha_3_final['ICMS_TOT_SAIDA_ORIG'].fillna(0) != 0,
                                        ficha_3_final['ICMS_TOT_SAIDA_ORIG'] * (1 + meta_fator),
                                        0)

    df_fim = ficha_3.copy()
    df_fim = df_fim.sort_values(by=['COD_ITEM', 'DATA', 'IND_OPER', 'SUB_TIPO'], 
                                    ascending=[True, True, True, True]).reset_index().drop(['index'], axis=1)

    df_fim['ICMS_TOT_FINAL'] = ficha_3_final['ICMS_TOT_FINAL']
    df_fim['VALOR_ICMS_OP_FINAL'] = ficha_3_final['VALOR_ICMS_OP_FINAL']
    df_fim['ICMS_TOT_SAIDA_FINAL'] = ficha_3_final['ICMS_TOT_SAIDA_FINAL']

    df_fim.rename(columns={'ICMS_TOT': 'ICMS_TOT_ORIG',
                            'ICMS_TOT_FINAL': 'ICMS_TOT',
                            'ICMS_TOT_SAIDA': 'ICMS_TOT_SAIDA_ORIG',
                            'ICMS_TOT_SAIDA_FINAL': 'ICMS_TOT_SAIDA',
                            'VLR_CONF_0': 'VL_CONFR_0',
                            'Valor ICMS Operação': 'Valor ICMS Operação Orig',
                            'VALOR_ICMS_OP_FINAL': 'Valor ICMS Operação'
                            }, inplace=True)

    df_fim = df_fim[['CHV_DOC', 'DATA', 'CFOP', 'NUM_ITEM', 'COD_ITEM', 'IND_OPER', 'SUB_TIPO', 'QTD_CAT',
                            'ICMS_TOT','COD_LEGAL', 'VL_CONFR_0', 'ICMS_TOT_SAIDA',
                            'ALIQUOTA', 'VALOR', 'Valor Base Cálculo ICMS ST Retido Operação Anterior',
                            'Valor Complementar', 'Valor ICMS Substituição Tributária', 'Valor ICMS Operação',
                            'CNPJ EMITENTE', 'vBCST', 'CST', 'Valor ICMS Operação Orig', 'ICMS_TOT_ORIG','ICMS_TOT_SAIDA_ORIG',
                            'FONTE']]
    
    ficha_3_fim = calcular_ressarcimento(df_fim)
    
    return ficha_3_fim

def gti_pra_baixo(tabela_2, ficha_3, meta_ressarc):
    '''
    Função para aplicação de GTI para lojas
    com ressarcimento acima da porcentagem estabelecida
    como meta
    
    '''

    # Cálculo de ressarcimento por produto
    ressarc = ficha_3['VLR_RESSARCIMENTO'].sum()
    diferenca = ressarc-meta_ressarc
    pivot_ressarc = ficha_3.pivot_table(index='COD_ITEM', values='VLR_RESSARCIMENTO', aggfunc='sum')
    pivot_ressarc = pivot_ressarc[pivot_ressarc['VLR_RESSARCIMENTO'] > 0].sort_values(by='VLR_RESSARCIMENTO',
                                                                                     ascending=True)

    # Definição dos produtos a terem código legal mudado de 1 para 0 e valor de confronto para nulo
    soma = 0
    i = 0
    cods = []
    while i < len(pivot_ressarc) and (soma + pivot_ressarc['VLR_RESSARCIMENTO'].iloc[i]) <= diferenca:
        soma += pivot_ressarc['VLR_RESSARCIMENTO'].iloc[i]
        cods.append(pivot_ressarc.index[i])
        i += 1
    
    tabela_2['COD_LEGAL'] = np.where(tabela_2['COD_ITEM'].isin(cods),
                              np.where(tabela_2['COD_LEGAL'].isnull(), tabela_2['COD_LEGAL'], 0),
                              tabela_2['COD_LEGAL'])
    
    tabela_2['VL_CONFR_0'] = np.where(tabela_2['COD_ITEM'].isin(cods),
                               np.nan,
                               tabela_2['VL_CONFR_0'])
    
    ficha_3_final = calcular_ressarcimento(tabela_2)
    
    return ficha_3_final