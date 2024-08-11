import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt

def analyse():
    data = pd.read_csv('../data/air_pollution_combined.csv')

    print(data.dtypes)

    data['date_time'] = pd.to_datetime(data['date_time'])

    numeric_data = data.select_dtypes(include=['float64', 'int64'])

    correlation_matrix = numeric_data.corr()
    plt.figure(figsize=(12, 8))
    sns.heatmap(correlation_matrix, annot=True, cmap='coolwarm', fmt='.2f')
    plt.title('Matrice de Corrélations')
    plt.savefig('../images/correlation_matrix.png')
    plt.close()

    cities = ['Paris', 'Los Angeles', 'Lima', 'Nairobi', 'Tokyo', 'Antananarivo']

    pollutants = ['pm2_5', 'pm10', 'co', 'no', 'no2', 'o3', 'so2', 'nh3']

    for city in cities:
        city_data = data[data['Location'] == city]

        plt.figure(figsize=(14, 10))
        for pollutant in pollutants:
            if pollutant in city_data.columns:
                plt.plot(city_data['date_time'], city_data[pollutant], label=pollutant)
        plt.xlabel('Date')
        plt.ylabel('Niveau de Pollution')
        plt.title(f'Niveaux de Pollution à {city} au Fil du Temps')
        plt.legend()
        plt.savefig(f'../images/pollution_time_series_{city}.png')
        plt.close()

    plt.figure(figsize=(14, 10))
    for pollutant in pollutants:
        plt.subplot(len(pollutants), 1, pollutants.index(pollutant) + 1)
        sns.boxplot(x='Location', y=pollutant, data=data)
        plt.title(f'Comparaison des Niveaux de {pollutant.upper()} entre Différentes Villes')
        plt.xlabel('Ville')
        plt.ylabel(pollutant.upper())
        plt.xticks(rotation=45)
    plt.tight_layout()
    plt.savefig('../images/pollutant_comparison.png')
    plt.close()
