import asyncio
from viam.module.module import Module
from models.audio_in import AudioIn as AudioInModel


if __name__ == '__main__':
    asyncio.run(Module.run_from_registry())
