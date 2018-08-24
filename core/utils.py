from threading import Thread


def start_thread(target, args=()):
    t = Thread(target=target, args=args)
    t.daemon = True
    t.start()
    return t
