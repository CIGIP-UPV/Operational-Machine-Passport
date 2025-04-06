import asyncio
import logging
import random
from asyncua import Server, ua


async def main():
    _logger = logging.getLogger("asyncua")

    # Inicializar servidor
    server = Server()
    await server.init()
    server.set_endpoint("opc.tcp://0.0.0.0:4840/freeopcua/cnc/")

    # Namespace
    uri = "http://examples.freeopcua.github.io/cnc"
    idx = await server.register_namespace(uri)

    # Crear árbol de nodos
    root = await server.nodes.objects.add_object(idx, "CNC_Machine")

    status = await root.add_object(idx, "Status")
    sensors = await root.add_object(idx, "Sensors")
    production = await root.add_object(idx, "Production")

    # Variables de estado
    operating_mode = await status.add_variable(f"ns={idx};s=Status.OperatingMode", "OperatingMode", ua.Variant("Auto", ua.VariantType.String))
    program_running = await status.add_variable(f"ns={idx};s=Status.ProgramRunning", "ProgramRunning", True)
    alarm_active = await status.add_variable(f"ns={idx};s=Status.AlarmActive", "AlarmActive", False)
    emergency_stop = await status.add_variable(f"ns={idx};s=Status.EmergencyStop", "EmergencyStop", False)
    spindle_running = await status.add_variable(f"ns={idx};s=Status.SpindleRunning", "SpindleRunning", True)

    # Sensores
    spindle_temp = await sensors.add_variable(f"ns={idx};s=Sensors.SpindleTemperature", "SpindleTemperature", 55.0)
    coolant_level = await sensors.add_variable(f"ns={idx};s=Sensors.CoolantLevel", "CoolantLevel", 80.0)
    vibration_level = await sensors.add_variable(f"ns={idx};s=Sensors.VibrationLevel", "VibrationLevel", 0.02)
    feed_rate = await sensors.add_variable(f"ns={idx};s=Sensors.FeedRate", "FeedRate", 1000.0)
    spindle_speed = await sensors.add_variable(f"ns={idx};s=Sensors.SpindleSpeed", "SpindleSpeed", 2800)
    axis_load_x = await sensors.add_variable(f"ns={idx};s=Sensors.AxisLoad_X", "AxisLoad_X", 40.0)
    axis_load_y = await sensors.add_variable(f"ns={idx};s=Sensors.AxisLoad_Y", "AxisLoad_Y", 35.0)
    axis_load_z = await sensors.add_variable(f"ns={idx};s=Sensors.AxisLoad_Z", "AxisLoad_Z", 50.0)

    # Producción
    parts_produced = await production.add_variable(f"ns={idx};s=Production.PartsProduced", "PartsProduced", 0)
    cycle_time = await production.add_variable(f"ns={idx};s=Production.CycleTime", "CycleTime", 20.0)
    total_runtime = await production.add_variable(f"ns={idx};s=Production.TotalRuntime", "TotalRuntime", 0)
    tool_number = await production.add_variable(f"ns={idx};s=Production.ToolNumber", "ToolNumber", 1)
    tool_wear = await production.add_variable(f"ns={idx};s=Production.ToolWear", "ToolWear", 5.0)

    # Marcar como escribibles (opcionales)
    for var in [spindle_temp, coolant_level, vibration_level, feed_rate, spindle_speed,
                axis_load_x, axis_load_y, axis_load_z, cycle_time, tool_wear,
                parts_produced, total_runtime, tool_number]:
        await var.set_writable()

    _logger.info("Starting CNC simulator OPC-UA server...")
    async with server:
        while True:
            await asyncio.sleep(1)

            # Simulación dinámica
            await spindle_temp.write_value(random.uniform(50.0, 80.0))
            await coolant_level.write_value(random.uniform(60.0, 100.0))
            await vibration_level.write_value(random.uniform(0.01, 0.05))
            await feed_rate.write_value(random.uniform(800.0, 1200.0))
            await spindle_speed.write_value(random.randint(1500, 3000))
            await axis_load_x.write_value(random.uniform(30.0, 60.0))
            await axis_load_y.write_value(random.uniform(30.0, 60.0))
            await axis_load_z.write_value(random.uniform(30.0, 60.0))
            await tool_wear.write_value(min(await tool_wear.get_value() + random.uniform(0.1, 0.3), 100.0))

            # Producción acumulativa
            current_parts = await parts_produced.get_value()
            await parts_produced.write_value(current_parts + 1)

            current_runtime = await total_runtime.get_value()
            await total_runtime.write_value(current_runtime + 1)

            _logger.debug("Updated CNC variables")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    asyncio.run(main())
