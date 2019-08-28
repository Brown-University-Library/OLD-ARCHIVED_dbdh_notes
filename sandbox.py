from configparser import RawConfigParser

cfg = RawConfigParser()
cfg.read('sandbox.ini')

print(dict(cfg))
print(cfg.get('section 2', 'whatever'))