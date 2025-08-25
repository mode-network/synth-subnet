import requests


import numpy as np
from scipy import stats


# Hermes Pyth API documentation: https://hermes.pyth.network/docs/

TOKEN_MAP = {
    "BTC": "e62df6c8b4a85fe1a67db44dc12de5db330f7ac66b72dc658afedf0f4a415b43",
    "ETH": "ff61491a931112ddf1bd8147cd1b641375f79f5825126d665480874634fd0ace",
    "XAU": "765d2ba906dbc32ca17cc11f5310a89e9ee1f6420508c63861f2f8ba4ee34bb2",
    "SOL": "ef0d8b6fda2ceba41da15d4095d1da392a0d2f8ed0c6c7bc0f4cfac8c280b56d",
}

pyth_base_url = "https://hermes.pyth.network/v2/updates/price/latest"


def get_asset_price(asset="BTC"):
    pyth_params = {"ids[]": [TOKEN_MAP[asset]]}
    response = requests.get(pyth_base_url, params=pyth_params)
    if response.status_code != 200:
        print("Error in response of Pyth API")
        return

    data = response.json()
    parsed_data = data.get("parsed", [])

    asset = parsed_data[0]
    price = int(asset["price"]["price"])
    expo = int(asset["price"]["expo"])

    live_price = price * (10**expo)

    return live_price


def simulate_single_price_path(
    current_price, time_increment, time_length, sigma
):
    """
    Simulate a single crypto asset price path.
    """
    one_hour = 3600
    dt = time_increment / one_hour
    num_steps = int(time_length / time_increment)
    std_dev = sigma * np.sqrt(dt)
    price_change_pcts = np.random.normal(0, std_dev, size=num_steps)
    cumulative_returns = np.cumprod(1 + price_change_pcts)
    cumulative_returns = np.insert(cumulative_returns, 0, 1.0)
    price_path = current_price * cumulative_returns
    return price_path


def simulate_single_price_path_advanced(
    current_price, 
    time_increment, 
    time_length, 
    sigma,
    asset="BTC"
):
    """
    Продвинутая модель симуляции ценового пути.
    
    Включает:
    - Авторегрессию (AR) - влияние предыдущих доходностей
    - Кластеризацию волатильности (GARCH-подобную)
    - Жирные хвосты (t-распределение)
    - Асимметричный эффект волатильности
    - Параметры, специфичные для каждого актива
    """
    one_hour = 3600
    dt = time_increment / one_hour
    num_steps = int(time_length / time_increment)
    
    # Параметры для каждого актива
    asset_params = {
        "BTC": {
            "ar_coef": 0.05,           # Коэффициент авторегрессии
            "vol_persistence": 0.85,    # Персистентность волатильности
            "vol_mean_reversion": 0.1,  # Скорость возврата к средней
            "df": 4.0,                 # Степени свободы для t-распределения
            "asym_coef": -0.1          # Асимметрия (отрицательные шоки)
        },
        "ETH": {
            "ar_coef": 0.08, 
            "vol_persistence": 0.80, 
            "vol_mean_reversion": 0.12, 
            "df": 4.5, 
            "asym_coef": -0.08
        },
        "XAU": {
            "ar_coef": 0.02, 
            "vol_persistence": 0.90, 
            "vol_mean_reversion": 0.05, 
            "df": 6.0, 
            "asym_coef": -0.05
        },
        "SOL": {
            "ar_coef": 0.12, 
            "vol_persistence": 0.75, 
            "vol_mean_reversion": 0.15, 
            "df": 3.5, 
            "asym_coef": -0.12
        },
    }
    
    params = asset_params.get(asset, asset_params["BTC"])
    
    # Инициализация массивов
    returns = np.zeros(num_steps)
    volatility = np.full(num_steps, sigma * np.sqrt(dt))
    
    # Генерируем путь с изменяющейся волатильностью
    for i in range(num_steps):
        # Обновляем волатильность (GARCH-подобная модель)
        if i > 0:
            # Избыточный шок от предыдущего периода
            shock = abs(returns[i-1]) - np.sqrt(dt) * sigma * 0.5
            
            # Новая волатильность = возврат к среднему + персистентность + шоки
            volatility[i] = (
                params["vol_mean_reversion"] * sigma * np.sqrt(dt) +
                params["vol_persistence"] * volatility[i-1] +
                0.1 * max(0, shock) +  # Положительные шоки увеличивают волатильность
                params["asym_coef"] * min(0, returns[i-1])  # Отрицательные доходности увеличивают волатильность больше
            )
            # Гарантируем положительную волатильность
            volatility[i] = max(0.001, volatility[i])
        
        # Генерируем доходность с авторегрессией
        ar_component = 0
        if i > 0:
            ar_component = params["ar_coef"] * returns[i-1]
        
        # Случайный шок из t-распределения (жирные хвосты)
        random_shock = stats.t.rvs(df=params["df"], scale=volatility[i])
        returns[i] = ar_component + random_shock
    
    # Преобразуем доходности в цены
    cumulative_returns = np.cumprod(1 + returns)
    cumulative_returns = np.insert(cumulative_returns, 0, 1.0)
    price_path = current_price * cumulative_returns
    
    return price_path


def simulate_crypto_price_paths(
    current_price, time_increment, time_length, num_simulations, sigma, asset="BTC"
):
    """
    Simulate multiple crypto asset price paths.
    """

    price_paths = []
    for _ in range(num_simulations):
        price_path = simulate_single_price_path_advanced(
            current_price, time_increment, time_length, sigma
        )
        price_paths.append(price_path)

    return np.array(price_paths)
