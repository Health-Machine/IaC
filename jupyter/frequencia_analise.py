import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd
from sklearn.linear_model import LinearRegression
from sklearn.metrics import r2_score

def prever_e_plotar(df):
    df['data_captura'] = pd.to_datetime(df['data_captura'], format='%d/%m/%Y %H:%M', errors='coerce')
    
    df = df.dropna(subset=['data_captura'])
    
    df = df.sort_values(by='data_captura').copy()

    # variavel de tempo em minutos a partir da data inicial
    df['minuto'] = (df['data_captura'] - df['data_captura'].min()).dt.total_seconds() / 60
    X = df[['minuto']]
    y = df['valor']

    modelo = LinearRegression()
    modelo.fit(X, y)
    y_pred = modelo.predict(X)

    coef = modelo.coef_[0]
    intercep = modelo.intercept_
    r2 = r2_score(y, y_pred)

    minuto_alvo = (40 - intercep) / coef
    tempo_previsto = df['data_captura'].min() + pd.to_timedelta(round(minuto_alvo), unit='m')

    print(f"Tendência (coeficiente da regressão): {coef:.4f}")
    print(f"R² da regressão: {r2:.4f}")
    print(f"Valor do sensor pode chegar a 40 aproximadamente em: {tempo_previsto.strftime('%d/%m/%Y %H:%M')}")

    sns.set(style="whitegrid")
    plt.figure(figsize=(12, 6))
    plt.plot(df['data_captura'], y, label='Valor real do sensor', marker='o')
    plt.plot(df['data_captura'], y_pred, color='red', linestyle='--', label='Regressão linear')
    plt.axhline(y=40, color='gray', linestyle=':', label='Limite = 40')
    plt.axvline(x=tempo_previsto, color='green', linestyle=':', label='Previsão de atingir 40')
    plt.title(f'Regressão Linear do Valor do Sensor\nTendência: {coef:.4f} | R²: {r2:.4f}')
    plt.xlabel('Data de Captura')
    plt.ylabel('Valor do Sensor')
    plt.legend()
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.show()


df_subset = df[:300].copy()
prever_e_plotar(df_subset)

# pip install scikit-learn
