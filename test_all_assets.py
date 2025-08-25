from datetime import datetime
from synth.miner.simulations import generate_simulations
from synth.simulation_input import SimulationInput
from synth.utils.helpers import get_current_time, round_time_to_minutes
from synth.validator.response_validation import validate_responses

assets = ["BTC", "ETH", "XAU", "SOL"]

for asset in assets:
    print(f"\n=== Тестируем {asset} ===")
    
    simulation_input = SimulationInput(
        asset=asset,
        time_increment=300,
        time_length=86400,
        num_simulations=100,
    )
    
    current_time = get_current_time()
    start_time = round_time_to_minutes(current_time, 60, 120)
    simulation_input.start_time = start_time.isoformat()
    
    print(f"Актив: {asset}")
    print(f"Время старта: {simulation_input.start_time}")
    
    try:
        prediction = generate_simulations(
            simulation_input.asset,
            start_time=simulation_input.start_time,
            time_increment=simulation_input.time_increment,
            time_length=simulation_input.time_length,
            num_simulations=simulation_input.num_simulations,
        )
        
        validation = validate_responses(
            prediction,
            simulation_input,
            datetime.fromisoformat(simulation_input.start_time),
            "0",
        )
        
        print(f"Результат валидации: {validation}")
        print(f"Количество путей: {len(prediction)}")
        print(f"Точек в каждом пути: {len(prediction[0])}")
        print(f"Первая цена: {prediction[0][0]['price']:.2f}")
        print(f"Последняя цена пути 1: {prediction[0][-1]['price']:.2f}")
        
    except Exception as e:
        print(f"ОШИБКА для {asset}: {e}")

print(f"\n=== Тестирование завершено ===")
