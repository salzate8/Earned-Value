import pandas as pd
import numpy as np

folder = '/Users/ramonalzate/Downloads/9. Valor Ganado/data/processed/'
#folder = 'C:/Users/salzate/Downloads/9. Valor Ganado/data/processed/'

file_ac = 'AC.xlsx'
sheet = 'rpt_PROY_CON_ComprasGP'
sumV=0
Acum=0
AcumPro=0
AcumAp=0
j=0
pAp=0
WWb=''

 ## Leer archivos df_ac.xlsx y df_wbs.xlsx
df_Ac = pd.read_excel(folder+'df_ac.xlsx', sheet_name='ac')
df_Wbs = pd.read_excel(folder+'df_wbs.xlsx', sheet_name='wbs')

#Crear la columna AC, Avance Real, AcAcum, PV y PE  en df_Wbs y la columna Pro en df_Ac, esta ultima para ir identicar cuales
#valores de trasldan
df_Wbs['AvanceReal'] = 0.0
df_Wbs['AC'] = 0.0
df_Wbs['AcAcum']=0.0
df_Wbs['PV']=0.0
df_Wbs['EV']=0.0
df_Wbs['CV'] = 0.0
df_Wbs['SV'] = 0.0
df_Wbs['CPI'] = 0.0 	
df_Wbs['SPI'] = 0.0 	 
df_Wbs['EAC'] = 0.0 	
df_Wbs['ETC'] = 0.0 	 
df_Wbs['VAC'] = 0.0
df_Wbs['EACT']=0.0
df_Wbs['DifDías']=0.0
df_Wbs['FechaFin']=df_Wbs['Fecha']


df_Ac['Proc'] = 'No'

#Se identifican y eliminan la ultima fila que contiene plan
freq=df_Wbs['plan_or_real'].value_counts()
freq=freq['plan']
print(freq)

#Se traslada a la columna AvanceReal los valores de ejeucion real 
for i in range(len(df_Wbs)):
    df_Wbs.loc[i, 'AvanceReal'] = df_Wbs.loc[i+freq, 'Avance']
    if i==freq-1:
        break
#Se borran las filas despues de la ultima que contiene Plan
df_Wbs.drop(df_Wbs.index[freq:freq*2], inplace=True) 

#Se cambia el nombre de la columna Avance por AvancePlan
df_Wbs = df_Wbs.rename(columns={'Avance': 'AvancePlan'})

#Se borran las filas inferiores relacionadas con ejecucion real
df_Wbs= df_Wbs.drop ('plan_or_real', axis = 1)



#Se crea un dataframe con las actividades para almacenar los acumulados de cada actividad
SWbs=df_Wbs['WBS'].unique()
SWbs=pd.DataFrame(SWbs)
SWbs['Acum']=0.0
SWbs = SWbs.rename(columns={0: 'WBS'})

#print(df_Wbs)

#Corregir la fecha en df_Wbs. No se hace esto porque estas filas se eliminan
#for i in range(len(df_Wbs)):
    #if df_Wbs.loc[i,'plan_or_real']=='real':
        #df_Wbs.loc[i,'Fecha']=df_Wbs.loc[i,'Fecha'][:19]
        #df_Wbs.loc[i,'Fecha']=pd.to_datetime( df_Wbs.loc[i,'Fecha'])


#Para trasladar los valores se intento agrupar por WBS y Fecha, luego filtral en bf_Wbs por estos mismos 
#valores y asignar el valor de la agrupacion, no se logro porque no pude trasformar la serie con multiindice 
#en una dataframe
#print(df_Wbs.dtypes)
#Ac_ag = df_Ac.groupby(['Fecha', 'WBS_0'])['Valor'].sum()
#print(Ac_ag)
#Ac_ag.reset_index()
#print(Ac_ag)
#AcA=Ac_ag.to_frame()
#print(AcA)


#df_Wbs['Comienzo']=df_Wbs['Comienzo'][4:]
#print(df_Wbs['Comienzo'])

#df_Wbs.loc[0,'Comienzo']=pd.to_datetime(df_Wbs.loc[0,'Comienzo'][4:], dayfirst=True)
#print(df_Wbs.loc[0,'Comienzo'])
#locd=df_Wbs.loc[0,'Duración'].find('d')-1
#df_Wbs.loc[0,'Duración']=df_Wbs.loc[0,'Duración'][:locd]
#df_Wbs.loc[0,'Duración']=pd.to_numeric(df_Wbs.loc[0,'Duración'])
#df_Wbs.loc[0,'Duración']=df_Wbs.loc[0,'Duración'].astype(int)
#print(df_Wbs.loc[0,'Duración'])

#Se realica la iteracion por cada elemento de df_Wbs para realizar el traslado y llenar las columnas de AC y acumulados
for i in range(len(df_Wbs)):
    print(i,df_Wbs.loc[i,'WBS'])
    #lectura de los acumulados actuales para el proyecto, la actividad y la actividad principal
    #Se Identifica la localizacion de la actividad y la actividad principal dentro del df SWbs
    AcumPro=SWbs.loc[0,'Acum']
    
    for j in range(len(SWbs)):    
        if df_Wbs.loc[i,'WBS'][:9]==SWbs.loc[j,'WBS']:
            AcumAp=SWbs.loc[j,'Acum']
            pAp=j
        if SWbs.loc[j,'WBS']==df_Wbs.loc[i,'WBS']:
            Acum=SWbs.loc[j,'Acum']
            break
            
    #Se selecciona que actividades ingresan para la busqueda de registros en df_Ac
    if (len(df_Wbs.loc[i,'WBS'])>9 and df_Wbs.loc[i,'WBS'][:6]!='3616_H') or (df_Wbs.loc[i,'WBS']=='3616_SP01'):
        AnWb=(df_Wbs.loc[i,'Fecha']).year
        MeWb=(df_Wbs.loc[i,'Fecha']).month
        DiWb=(df_Wbs.loc[i,'Fecha']).day
        WWb=df_Wbs.loc[i,'WBS']

        sumV=0
        #Se ingresa a df_Ac a buscar los registros del respectivo df_Wbs por fecha y WBS.
        for y in range(len(df_Ac)):
            if df_Ac.loc[y,'Proc'] == 'No':
                AnAc=(df_Ac.loc[y,'Fecha']).year
                MeAc=(df_Ac.loc[y,'Fecha']).month
                DiAC=(df_Ac.loc[y,'Fecha']).day
                WAc=df_Ac.loc[y,'WBS_0']
                if WAc[5:9]=="SP01":
                    WAc=WAc[:9]
                
                if DiAC<16 and AnWb==AnAc and MeWb==MeAc and DiWb==15 and WAc==WWb:

                    df_Wbs.loc[i,'AC']=df_Wbs.loc[i,'AC']+(df_Ac.loc[y,'Valor'])
                    df_Wbs.loc[i,'AcAcum']= Acum+df_Ac.loc[y,'Valor']
                    if df_Wbs.loc[i,'WBS']!='3616_SP01':
                        df_Wbs.loc[i-(j-pAp),'AcAcum']= AcumAp+df_Ac.loc[y,'Valor']
                        #df_Wbs.loc[i-(j-pAp),'AC']= df_Wbs.loc[i-(j-pAp),'AC']+df_Ac.loc[y,'Valor']
                    df_Wbs.loc[i-j,'AcAcum']= AcumPro+df_Ac.loc[y,'Valor']
                    #df_Wbs.loc[i-j,'AC']= df_Wbs.loc[i-j,'AC']+df_Ac.loc[y,'Valor']
                    SWbs.loc[j,'Acum']=SWbs.loc[j,'Acum']+df_Ac.loc[y,'Valor']
                    Acum=SWbs.loc[j,'Acum']
                    SWbs.loc[pAp,'Acum']=SWbs.loc[pAp,'Acum']+df_Ac.loc[y,'Valor']
                    AcumAp=SWbs.loc[pAp,'Acum']
                    SWbs.loc[0,'Acum']=SWbs.loc[0,'Acum']+df_Ac.loc[y,'Valor']
                    AcumPro=SWbs.loc[0,'Acum']
                    df_Ac.loc[y,'Proc'] = 'Si'
                    sumV =1

                elif DiAC>15 and AnWb==AnAc and MeWb==MeAc and DiWb==30 and WAc==WWb:
                    df_Wbs.loc[i,'AC']=df_Wbs.loc[i,'AC']+(df_Ac.loc[y,'Valor'])
                    df_Wbs.loc[i,'AcAcum']= Acum+df_Ac.loc[y,'Valor']
                    if df_Wbs.loc[i,'WBS']!='3616_SP01':
                        df_Wbs.loc[i-(j-pAp),'AcAcum']= AcumAp+df_Ac.loc[y,'Valor']
                        #df_Wbs.loc[i-(j-pAp),'AC']= df_Wbs.loc[i-(j-pAp),'AC']+df_Ac.loc[y,'Valor']
                    df_Wbs.loc[i-j,'AcAcum']= AcumPro+df_Ac.loc[y,'Valor']
                    #df_Wbs.loc[i-j,'AC']= df_Wbs.loc[i-j,'AC']+df_Ac.loc[y,'Valor']
                    SWbs.loc[j,'Acum']=SWbs.loc[j,'Acum']+df_Ac.loc[y,'Valor']
                    Acum=SWbs.loc[j,'Acum']
                    SWbs.loc[pAp,'Acum']=SWbs.loc[pAp,'Acum']+df_Ac.loc[y,'Valor']
                    AcumAp=SWbs.loc[pAp,'Acum']
                    SWbs.loc[0,'Acum']=SWbs.loc[0,'Acum']+df_Ac.loc[y,'Valor']
                    AcumPro=SWbs.loc[0,'Acum']
                    df_Ac.loc[y,'Proc'] = 'Si'
                    sumV =1
                elif DiAC>15 and AnWb==AnAc and MeWb==MeAc and DiWb==28 and WAc==WWb:
                    df_Wbs.loc[i,'AC']=df_Wbs.loc[i,'AC']+(df_Ac.loc[y,'Valor'])
                    df_Wbs.loc[i,'AcAcum']= Acum+df_Ac.loc[y,'Valor']
                    if df_Wbs.loc[i,'WBS']!='3616_SP01':
                        df_Wbs.loc[i-(j-pAp),'AcAcum']= AcumAp+df_Ac.loc[y,'Valor']
                        #df_Wbs.loc[i-(j-pAp),'AC']= df_Wbs.loc[i-(j-pAp),'AC']+df_Ac.loc[y,'Valor']
                    df_Wbs.loc[i-j,'AcAcum']= AcumPro+df_Ac.loc[y,'Valor']
                    #df_Wbs.loc[i-j,'AC']= df_Wbs.loc[i-j,'AC']+df_Ac.loc[y,'Valor']
                    SWbs.loc[j,'Acum']=SWbs.loc[j,'Acum']+df_Ac.loc[y,'Valor']
                    Acum=SWbs.loc[j,'Acum']
                    SWbs.loc[pAp,'Acum']=SWbs.loc[pAp,'Acum']+df_Ac.loc[y,'Valor']
                    AcumAp=SWbs.loc[pAp,'Acum']
                    SWbs.loc[0,'Acum']=SWbs.loc[0,'Acum']+df_Ac.loc[y,'Valor']
                    AcumPro=SWbs.loc[0,'Acum']
                    df_Ac.loc[y,'Proc'] = 'Si'
                    sumV =1
    #else:
        #print('no se procesa')
    
    #Se identifica si la actividad encontro valores o no.  En caso de que no haya encontrado valores se asignan los acumulados
    if  sumV==0:
        if (len(df_Wbs.loc[i,'WBS'])>=6 and (df_Wbs.loc[i,'WBS'])[:6]=='3616_H') or (df_Wbs.loc[i,'WBS']=='3616_'):
            sumV=0
        else:
            #if df_Wbs.loc[i,'WBS']=='3616_H000_0':
                #print(df_Wbs.loc[i,'WBS'])
            df_Wbs.loc[i,'AcAcum']= Acum
            df_Wbs.loc[i-(j-pAp),'AcAcum']= AcumAp
            df_Wbs.loc[i-j,'AcAcum']= AcumPro
#Se realizan los calculos para las columnas PV y PE 
for i in range(len(df_Wbs)):
    print(i)
    #Convertir comienzo y fin a formato fecha
    df_Wbs.loc[i,'Comienzo']=pd.to_datetime(df_Wbs.loc[i,'Comienzo'][4:], dayfirst=True)
    df_Wbs.loc[i,'Fin']=pd.to_datetime(df_Wbs.loc[i,'Fin'][4:], dayfirst=True)
    
    #Convertir duracion a un entero
    locd=df_Wbs.loc[i,'Duración'].find('d')-1
    df_Wbs.loc[i,'Duración']=df_Wbs.loc[i,'Duración'][:locd]
    df_Wbs.loc[i,'Duración']=pd.to_numeric(df_Wbs.loc[i,'Duración'])


    df_Wbs.loc[i,'PV']= df_Wbs.loc[i,'AvancePlan']*df_Wbs.loc[i,'LB Costo COP']
    df_Wbs.loc[i,'EV']= df_Wbs.loc[i,'AvanceReal']*df_Wbs.loc[i,'LB Costo COP']
    df_Wbs.loc[i,'CV'] = df_Wbs.loc[i,'EV'] - df_Wbs.loc[i,'AcAcum']
    df_Wbs.loc[i,'SV']= df_Wbs.loc[i,'EV'] - df_Wbs.loc[i,'PV']
   
    if df_Wbs.loc[i,'AcAcum']!=0.0:
        df_Wbs.loc[i,'CPI']= df_Wbs.loc[i,'EV']/ df_Wbs.loc[i,'AcAcum']
        if df_Wbs.loc[i,'CPI']>=2.0:
            df_Wbs.loc[i,'CPI']=1.0
    if df_Wbs.loc[i,'PV']!=0.0:
        df_Wbs.loc[i,'SPI']= df_Wbs.loc[i,'EV']/ df_Wbs.loc[i,'PV']
    if df_Wbs.loc[i,'CPI']!=0.0:
        df_Wbs.loc[i,'EAC']= df_Wbs.loc[i,'LB Costo COP']/ df_Wbs.loc[i,'CPI']
    df_Wbs.loc[i,'ETC']= df_Wbs.loc[i,'EAC'] - df_Wbs.loc[i,'AcAcum']
    df_Wbs.loc[i,'VAC']= df_Wbs.loc[i,'LB Costo COP'] - df_Wbs.loc[i,'EAC']
    
    if df_Wbs.loc[i,'SPI']!=0.0:
        #locd=df_Wbs.loc[i,'Duración'].find('d')-1
        df_Wbs.loc[i,'EACT']= df_Wbs.loc[i,'Duración']/ df_Wbs.loc[i,'SPI']
    
    if df_Wbs.loc[i,'EACT']==0.0:
        df_Wbs.loc[i,'DifDías']=0.0
    else:
        df_Wbs.loc[i,'DifDías']=df_Wbs.loc[i,'EACT']-df_Wbs.loc[i,'Duración']

    df_Wbs.loc[i, 'FechaFin'] =  df_Wbs.loc[i, 'Fin']+pd.Timedelta(days=df_Wbs.loc[i, 'DifDías'])


    

#df_Ac.to_excel('/Users/ramonalzate/Downloads/9. Valor Ganado/data/processed/df_Ac_pr.xlsx', sheet_name='Ac')
df_Wbs.to_excel('/Users/ramonalzate/Downloads/9. Valor Ganado/data/processed/df_wbs_pr.xlsx', sheet_name='wbs')

#Se trasladan a excel los resultados
#df_Ac.to_excel('C:/Users/salzate/Downloads/9. Valor Ganado/data/processed/df_Ac_pr.xlsx', sheet_name='Ac')
#df_Wbs.to_excel('C:/Users/salzate/Downloads/9. Valor Ganado/data/processed/df_wbs_pr.xlsx', sheet_name='wbs')

