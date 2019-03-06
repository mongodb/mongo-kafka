# MongoDB Kafka Connector

```
                Here be dragons

    Under heavy development as such change expect change and 
          this to be broken at *any* time.

                               ___, ____--'
                          _,-.'_,-'      (
                       ,-' _.-''....____(
             ,))_     /  ,'\ `'-.     (          /\
     __ ,+..a`  \(_   ) /   \    `'-..(         /  \
     )`-;...,_   \(_ ) /     \  ('''    ;'^^`\ <./\.>
         ,_   )   |( )/   ,./^``_..._  < /^^\ \_.))
        `=;; (    (/_')-- -'^^`      ^^-.`_.-` >-'
        `=\\ (                             _,./
          ,\`(                         )^^^
            ``;         __-'^^\       /
              / _>emj^^^   `\..`-.    ``'.
             / /               / /``'`; /
            / /          ,-=='-`=-'  / /
      ,-=='-`=-.               ,-=='-`=-.
    ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
```

## Credits

Original work by: Hans-Peter Grahsl : https://github.com/hpgrahsl/kafka-connect-mongodb

## IntelliJ IDEA

A couple of manual configuration steps are required to run the code in IntelliJ:

- **Error:** `java: cannot find symbol. symbol: variable Versions`<br>
 **Fixes:** Any of the following: <br>
 - Run the `compileBuildConfig` task: eg: `./gradlew compileBuildConfig` or via Gradle > mongo-kafka > Tasks > other > compileBuildConfig
 - Set `compileBuildConfig` to execute Before Build. via Gradle > Tasks > other > right click compileBuildConfig - click on "Execute Before Build" 
 - Delegate all build actions to Gradle: Settings > Build, Execution, Deployment > Build Tools > Gradle > Runner - tick "Delegate IDE build/run actions to gradle"

