from setuptools import setup

setup(
    name='rep_preload',
    version='0.0.4',
    packages=['core', 'receiver', 'util'],
    py_modules=['preload_reportd', 'receiverd'],
    scripts=['bin/startup.sh','bin/receiver.sh', 'bin/preload_report.sh'],
    entry_points={
        'console_scripts': [
            'receiverd=receiverd:main',
            'preload_reportd=preload_reportd:main'
        ]
    },
    include_package_data=True,
    zip_safe=False,
    install_requires=[

        'simplejson==3.16.0',
        'pymongo==3.7.1',
        'pika==0.12.0',
        'redis==2.10.6',
        'tornado==5.1.1',
        'aiohttp==3.5.4',
        # 'paramiko==1.15.2',
        #'requests==2.6.0',
        #'python-etcd==0.4.3',
        #'nose==1.3.4',
        #'pyOpenSSL==16.2.0',
        #'pycrypto==2.6.1'

    ]
)
