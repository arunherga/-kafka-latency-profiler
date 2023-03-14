def confi(args):
    conf = {
        'bootstrap.servers': args.bootstrap_servers,
        'group.id': args.group_id,
        "security.protocol": "SASL_SSL",
        "sasl.mechanisms": "PLAIN",
        "sasl.username": "4WSOEINJVRBJDMBB",
        "sasl.password": "J58R0DMyN8n1OMIiAj2wOpQ2WrePSO05YXQ63dwmzN7WPIG2UOphSf+rL4WX1WeW",
        'auto.offset.reset': args.auto_offset_reset,
        'enable.auto.commit': args.enable_auto_commit,
        'connections.max.idle.ms': 1200
    }
    return conf 