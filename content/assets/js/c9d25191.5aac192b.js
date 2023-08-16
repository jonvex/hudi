"use strict";(self.webpackChunkhudi=self.webpackChunkhudi||[]).push([[17213],{3905:(e,t,n)=>{n.d(t,{Zo:()=>d,kt:()=>h});var a=n(67294);function i(e,t,n){return t in e?Object.defineProperty(e,t,{value:n,enumerable:!0,configurable:!0,writable:!0}):e[t]=n,e}function r(e,t){var n=Object.keys(e);if(Object.getOwnPropertySymbols){var a=Object.getOwnPropertySymbols(e);t&&(a=a.filter((function(t){return Object.getOwnPropertyDescriptor(e,t).enumerable}))),n.push.apply(n,a)}return n}function o(e){for(var t=1;t<arguments.length;t++){var n=null!=arguments[t]?arguments[t]:{};t%2?r(Object(n),!0).forEach((function(t){i(e,t,n[t])})):Object.getOwnPropertyDescriptors?Object.defineProperties(e,Object.getOwnPropertyDescriptors(n)):r(Object(n)).forEach((function(t){Object.defineProperty(e,t,Object.getOwnPropertyDescriptor(n,t))}))}return e}function l(e,t){if(null==e)return{};var n,a,i=function(e,t){if(null==e)return{};var n,a,i={},r=Object.keys(e);for(a=0;a<r.length;a++)n=r[a],t.indexOf(n)>=0||(i[n]=e[n]);return i}(e,t);if(Object.getOwnPropertySymbols){var r=Object.getOwnPropertySymbols(e);for(a=0;a<r.length;a++)n=r[a],t.indexOf(n)>=0||Object.prototype.propertyIsEnumerable.call(e,n)&&(i[n]=e[n])}return i}var s=a.createContext({}),u=function(e){var t=a.useContext(s),n=t;return e&&(n="function"==typeof e?e(t):o(o({},t),e)),n},d=function(e){var t=u(e.components);return a.createElement(s.Provider,{value:t},e.children)},p="mdxType",c={inlineCode:"code",wrapper:function(e){var t=e.children;return a.createElement(a.Fragment,{},t)}},m=a.forwardRef((function(e,t){var n=e.components,i=e.mdxType,r=e.originalType,s=e.parentName,d=l(e,["components","mdxType","originalType","parentName"]),p=u(n),m=i,h=p["".concat(s,".").concat(m)]||p[m]||c[m]||r;return n?a.createElement(h,o(o({ref:t},d),{},{components:n})):a.createElement(h,o({ref:t},d))}));function h(e,t){var n=arguments,i=t&&t.mdxType;if("string"==typeof e||i){var r=n.length,o=new Array(r);o[0]=m;var l={};for(var s in t)hasOwnProperty.call(t,s)&&(l[s]=t[s]);l.originalType=e,l[p]="string"==typeof e?e:i,o[1]=l;for(var u=2;u<r;u++)o[u]=n[u];return a.createElement.apply(null,o)}return a.createElement.apply(null,n)}m.displayName="MDXCreateElement"},58215:(e,t,n)=>{n.d(t,{Z:()=>i});var a=n(67294);const i=function(e){let{children:t,hidden:n,className:i}=e;return a.createElement("div",{role:"tabpanel",hidden:n,className:i},t)}},26396:(e,t,n)=>{n.d(t,{Z:()=>c});var a=n(87462),i=n(67294),r=n(72389),o=n(79443);const l=function(){const e=(0,i.useContext)(o.Z);if(null==e)throw new Error('"useUserPreferencesContext" is used outside of "Layout" component.');return e};var s=n(53810),u=n(86010);const d={tabItem:"tabItem_vU9c"};function p(e){const{lazy:t,block:n,defaultValue:r,values:o,groupId:p,className:c}=e,m=i.Children.map(e.children,(e=>{if((0,i.isValidElement)(e)&&void 0!==e.props.value)return e;throw new Error(`Docusaurus error: Bad <Tabs> child <${"string"==typeof e.type?e.type:e.type.name}>: all children of the <Tabs> component should be <TabItem>, and every <TabItem> should have a unique "value" prop.`)})),h=o??m.map((e=>{let{props:{value:t,label:n,attributes:a}}=e;return{value:t,label:n,attributes:a}})),k=(0,s.lx)(h,((e,t)=>e.value===t.value));if(k.length>0)throw new Error(`Docusaurus error: Duplicate values "${k.map((e=>e.value)).join(", ")}" found in <Tabs>. Every value needs to be unique.`);const f=null===r?r:r??m.find((e=>e.props.default))?.props.value??m[0]?.props.value;if(null!==f&&!h.some((e=>e.value===f)))throw new Error(`Docusaurus error: The <Tabs> has a defaultValue "${f}" but none of its children has the corresponding value. Available values are: ${h.map((e=>e.value)).join(", ")}. If you intend to show no default tab, use defaultValue={null} instead.`);const{tabGroupChoices:g,setTabGroupChoices:b}=l(),[y,v]=(0,i.useState)(f),N=[],{blockElementScrollPositionUntilNextRender:T}=(0,s.o5)();if(null!=p){const e=g[p];null!=e&&e!==y&&h.some((t=>t.value===e))&&v(e)}const E=e=>{const t=e.currentTarget,n=N.indexOf(t),a=h[n].value;a!==y&&(T(t),v(a),null!=p&&b(p,a))},w=e=>{let t=null;switch(e.key){case"ArrowRight":{const n=N.indexOf(e.currentTarget)+1;t=N[n]||N[0];break}case"ArrowLeft":{const n=N.indexOf(e.currentTarget)-1;t=N[n]||N[N.length-1];break}}t?.focus()};return i.createElement("div",{className:"tabs-container"},i.createElement("ul",{role:"tablist","aria-orientation":"horizontal",className:(0,u.Z)("tabs",{"tabs--block":n},c)},h.map((e=>{let{value:t,label:n,attributes:r}=e;return i.createElement("li",(0,a.Z)({role:"tab",tabIndex:y===t?0:-1,"aria-selected":y===t,key:t,ref:e=>N.push(e),onKeyDown:w,onFocus:E,onClick:E},r,{className:(0,u.Z)("tabs__item",d.tabItem,r?.className,{"tabs__item--active":y===t})}),n??t)}))),t?(0,i.cloneElement)(m.filter((e=>e.props.value===y))[0],{className:"margin-vert--md"}):i.createElement("div",{className:"margin-vert--md"},m.map(((e,t)=>(0,i.cloneElement)(e,{key:t,hidden:e.props.value!==y})))))}function c(e){const t=(0,r.Z)();return i.createElement(p,(0,a.Z)({key:String(t)},e))}},72854:(e,t,n)=>{n.r(t),n.d(t,{contentTitle:()=>s,default:()=>m,frontMatter:()=>l,metadata:()=>u,toc:()=>d});var a=n(87462),i=(n(67294),n(3905)),r=n(26396),o=n(58215);const l={title:"Flink Guide",toc:!0,last_modified_at:new Date("2020-08-12T07:19:57.000Z")},s=void 0,u={unversionedId:"flink-quick-start-guide",id:"version-0.13.1/flink-quick-start-guide",title:"Flink Guide",description:"This page introduces Flink-Hudi integration. We can feel the unique charm of how Flink brings in the power of streaming into Hudi.",source:"@site/versioned_docs/version-0.13.1/flink-quick-start-guide.md",sourceDirName:".",slug:"/flink-quick-start-guide",permalink:"/docs/flink-quick-start-guide",editUrl:"https://github.com/apache/hudi/tree/asf-site/website/versioned_docs/version-0.13.1/flink-quick-start-guide.md",tags:[],version:"0.13.1",frontMatter:{title:"Flink Guide",toc:!0,last_modified_at:"2020-08-12T07:19:57.000Z"},sidebar:"docs",previous:{title:"Spark Guide",permalink:"/docs/quick-start-guide"},next:{title:"Docker Demo",permalink:"/docs/docker_demo"}},d=[{value:"Quick Start",id:"quick-start",children:[{value:"Setup",id:"setup",children:[{value:"Step.1 download Flink jar",id:"step1-download-flink-jar",children:[],level:4},{value:"Step.2 start Flink cluster",id:"step2-start-flink-cluster",children:[],level:4},{value:"Step.3 start Flink SQL client",id:"step3-start-flink-sql-client",children:[],level:4}],level:3},{value:"Insert Data",id:"insert-data",children:[],level:3},{value:"Query Data",id:"query-data",children:[],level:3},{value:"Update Data",id:"update-data",children:[],level:3},{value:"Streaming Query",id:"streaming-query",children:[],level:3},{value:"Delete Data",id:"deletes",children:[],level:3}],level:2},{value:"Where To Go From Here?",id:"where-to-go-from-here",children:[],level:2}],p={toc:d},c="wrapper";function m(e){let{components:t,...n}=e;return(0,i.kt)(c,(0,a.Z)({},p,n,{components:t,mdxType:"MDXLayout"}),(0,i.kt)("p",null,"This page introduces Flink-Hudi integration. We can feel the unique charm of how Flink brings in the power of streaming into Hudi.\nThis guide helps you quickly start using Flink on Hudi, and learn different modes for reading/writing Hudi by Flink:"),(0,i.kt)("ul",null,(0,i.kt)("li",{parentName:"ul"},(0,i.kt)("strong",{parentName:"li"},"Quick Start")," : Read ",(0,i.kt)("a",{parentName:"li",href:"#quick-start"},"Quick Start")," to get started quickly Flink sql client to write to(read from) Hudi."),(0,i.kt)("li",{parentName:"ul"},(0,i.kt)("strong",{parentName:"li"},"Configuration")," : For ",(0,i.kt)("a",{parentName:"li",href:"/docs/flink_configuration#global-configurations"},"Global Configuration"),", sets up through ",(0,i.kt)("inlineCode",{parentName:"li"},"$FLINK_HOME/conf/flink-conf.yaml"),". For per job configuration, sets up through ",(0,i.kt)("a",{parentName:"li",href:"/docs/flink_configuration#table-options"},"Table Option"),"."),(0,i.kt)("li",{parentName:"ul"},(0,i.kt)("strong",{parentName:"li"},"Writing Data")," : Flink supports different modes for writing, such as ",(0,i.kt)("a",{parentName:"li",href:"/docs/hoodie_deltastreamer#cdc-ingestion"},"CDC Ingestion"),", ",(0,i.kt)("a",{parentName:"li",href:"/docs/hoodie_deltastreamer#bulk-insert"},"Bulk Insert"),", ",(0,i.kt)("a",{parentName:"li",href:"/docs/hoodie_deltastreamer#index-bootstrap"},"Index Bootstrap"),", ",(0,i.kt)("a",{parentName:"li",href:"/docs/hoodie_deltastreamer#changelog-mode"},"Changelog Mode")," and ",(0,i.kt)("a",{parentName:"li",href:"/docs/hoodie_deltastreamer#append-mode"},"Append Mode"),"."),(0,i.kt)("li",{parentName:"ul"},(0,i.kt)("strong",{parentName:"li"},"Querying Data")," : Flink supports different modes for reading, such as ",(0,i.kt)("a",{parentName:"li",href:"/docs/querying_data#streaming-query"},"Streaming Query")," and ",(0,i.kt)("a",{parentName:"li",href:"/docs/querying_data#incremental-query"},"Incremental Query"),"."),(0,i.kt)("li",{parentName:"ul"},(0,i.kt)("strong",{parentName:"li"},"Tuning")," : For write/read tasks, this guide gives some tuning suggestions, such as ",(0,i.kt)("a",{parentName:"li",href:"/docs/flink_configuration#memory-optimization"},"Memory Optimization")," and ",(0,i.kt)("a",{parentName:"li",href:"/docs/flink_configuration#write-rate-limit"},"Write Rate Limit"),"."),(0,i.kt)("li",{parentName:"ul"},(0,i.kt)("strong",{parentName:"li"},"Optimization"),": Offline compaction is supported ",(0,i.kt)("a",{parentName:"li",href:"/docs/compaction#flink-offline-compaction"},"Offline Compaction"),"."),(0,i.kt)("li",{parentName:"ul"},(0,i.kt)("strong",{parentName:"li"},"Query Engines"),": Besides Flink, many other engines are integrated: ",(0,i.kt)("a",{parentName:"li",href:"/docs/syncing_metastore#flink-setup"},"Hive Query"),", ",(0,i.kt)("a",{parentName:"li",href:"/docs/querying_data#prestodb"},"Presto Query"),"."),(0,i.kt)("li",{parentName:"ul"},(0,i.kt)("strong",{parentName:"li"},"Catalog"),": A Hudi specific catalog is supported: ",(0,i.kt)("a",{parentName:"li",href:"/docs/querying_data/#hudi-catalog"},"Hudi Catalog"),".")),(0,i.kt)("h2",{id:"quick-start"},"Quick Start"),(0,i.kt)("h3",{id:"setup"},"Setup"),(0,i.kt)(r.Z,{defaultValue:"flinksql",values:[{label:"Flink SQL",value:"flinksql"},{label:"DataStream API",value:"dataStream"}],mdxType:"Tabs"},(0,i.kt)(o.Z,{value:"flinksql",mdxType:"TabItem"},(0,i.kt)("p",null,"We use the ",(0,i.kt)("a",{parentName:"p",href:"https://ci.apache.org/projects/flink/flink-docs-release-1.13/docs/dev/table/sqlclient/"},"Flink Sql Client")," because it's a good\nquick start tool for SQL users."),(0,i.kt)("h4",{id:"step1-download-flink-jar"},"Step.1 download Flink jar"),(0,i.kt)("p",null,"Hudi works with both Flink 1.13, Flink 1.14, Flink 1.15 and Flink 1.16. You can follow the\ninstructions ",(0,i.kt)("a",{parentName:"p",href:"https://flink.apache.org/downloads"},"here")," for setting up Flink. Then choose the desired Hudi-Flink bundle\njar to work with different Flink and Scala versions:"),(0,i.kt)("ul",null,(0,i.kt)("li",{parentName:"ul"},(0,i.kt)("inlineCode",{parentName:"li"},"hudi-flink1.13-bundle")),(0,i.kt)("li",{parentName:"ul"},(0,i.kt)("inlineCode",{parentName:"li"},"hudi-flink1.14-bundle")),(0,i.kt)("li",{parentName:"ul"},(0,i.kt)("inlineCode",{parentName:"li"},"hudi-flink1.15-bundle")),(0,i.kt)("li",{parentName:"ul"},(0,i.kt)("inlineCode",{parentName:"li"},"hudi-flink1.16-bundle"))),(0,i.kt)("h4",{id:"step2-start-flink-cluster"},"Step.2 start Flink cluster"),(0,i.kt)("p",null,"Start a standalone Flink cluster within hadoop environment.\nBefore you start up the cluster, we suggest to config the cluster as follows:"),(0,i.kt)("ul",null,(0,i.kt)("li",{parentName:"ul"},"in ",(0,i.kt)("inlineCode",{parentName:"li"},"$FLINK_HOME/conf/flink-conf.yaml"),", add config option ",(0,i.kt)("inlineCode",{parentName:"li"},"taskmanager.numberOfTaskSlots: 4")),(0,i.kt)("li",{parentName:"ul"},"in ",(0,i.kt)("inlineCode",{parentName:"li"},"$FLINK_HOME/conf/flink-conf.yaml"),", ",(0,i.kt)("a",{parentName:"li",href:"/docs/flink_configuration#global-configurations"},"add other global configurations according to the characteristics of your task")),(0,i.kt)("li",{parentName:"ul"},"in ",(0,i.kt)("inlineCode",{parentName:"li"},"$FLINK_HOME/conf/workers"),", add item ",(0,i.kt)("inlineCode",{parentName:"li"},"localhost")," as 4 lines so that there are 4 workers on the local cluster")),(0,i.kt)("p",null,"Now starts the cluster:"),(0,i.kt)("pre",null,(0,i.kt)("code",{parentName:"pre",className:"language-bash"},"# HADOOP_HOME is your hadoop root directory after unpack the binary package.\nexport HADOOP_CLASSPATH=`$HADOOP_HOME/bin/hadoop classpath`\n\n# Start the Flink standalone cluster\n./bin/start-cluster.sh\n")),(0,i.kt)("h4",{id:"step3-start-flink-sql-client"},"Step.3 start Flink SQL client"),(0,i.kt)("p",null,"Hudi supports packaged bundle jar for Flink, which should be loaded in the Flink SQL Client when it starts up.\nYou can build the jar manually under path ",(0,i.kt)("inlineCode",{parentName:"p"},"hudi-source-dir/packaging/hudi-flink-bundle"),"(see ",(0,i.kt)("a",{parentName:"p",href:"/docs/syncing_metastore#install"},"Build Flink Bundle Jar"),"), or download it from the\n",(0,i.kt)("a",{parentName:"p",href:"https://repo.maven.apache.org/maven2/org/apache/hudi/"},"Apache Official Repository"),"."),(0,i.kt)("p",null,"Now starts the SQL CLI:"),(0,i.kt)("pre",null,(0,i.kt)("code",{parentName:"pre",className:"language-bash"},"# HADOOP_HOME is your hadoop root directory after unpack the binary package.\nexport HADOOP_CLASSPATH=`$HADOOP_HOME/bin/hadoop classpath`\n\n./bin/sql-client.sh embedded -j .../hudi-flink-bundle_2.1?-*.*.*.jar shell\n")),(0,i.kt)("div",{className:"notice--info"},(0,i.kt)("h4",null,"Please note the following: "),(0,i.kt)("ul",null,(0,i.kt)("li",null,"We suggest hadoop 2.9.x+ version because some of the object storage has filesystem implementation only after that"),(0,i.kt)("li",null,"The flink-parquet and flink-avro formats are already packaged into the hudi-flink-bundle jar"))),(0,i.kt)("p",null,"Setup table name, base path and operate using SQL for this guide.\nThe SQL CLI only executes the SQL line by line.")),(0,i.kt)(o.Z,{value:"dataStream",mdxType:"TabItem"},(0,i.kt)("p",null,"Hudi works with Flink 1.13, Flink 1.14 and Flink 1.15. Please add the desired\ndependency to your project:"),(0,i.kt)("pre",null,(0,i.kt)("code",{parentName:"pre",className:"language-xml"},"\x3c!-- Flink 1.13 --\x3e\n<dependency>\n    <groupId>org.apache.hudi</groupId>\n    <artifactId>hudi-flink1.13-bundle</artifactId>\n    <version>0.13.1/version>\n</dependency>\n")),(0,i.kt)("pre",null,(0,i.kt)("code",{parentName:"pre",className:"language-xml"},"\x3c!-- Flink 1.14 --\x3e\n<dependency>\n    <groupId>org.apache.hudi</groupId>\n    <artifactId>hudi-flink1.14-bundle</artifactId>\n    <version>0.13.1</version>\n</dependency>\n")),(0,i.kt)("pre",null,(0,i.kt)("code",{parentName:"pre",className:"language-xml"},"\x3c!-- Flink 1.15 --\x3e\n<dependency>\n    <groupId>org.apache.hudi</groupId>\n    <artifactId>hudi-flink1.15-bundle</artifactId>\n    <version>0.13.1</version>\n</dependency>\n")),(0,i.kt)("pre",null,(0,i.kt)("code",{parentName:"pre",className:"language-xml"},"\x3c!-- Flink 1.16 --\x3e\n<dependency>\n    <groupId>org.apache.hudi</groupId>\n    <artifactId>hudi-flink1.16-bundle</artifactId>\n    <version>0.13.1</version>\n</dependency>\n")))),(0,i.kt)("h3",{id:"insert-data"},"Insert Data"),(0,i.kt)(r.Z,{defaultValue:"flinksql",values:[{label:"Flink SQL",value:"flinksql"},{label:"DataStream API",value:"dataStream"}],mdxType:"Tabs"},(0,i.kt)(o.Z,{value:"flinksql",mdxType:"TabItem"},(0,i.kt)("p",null,"Creates a Flink Hudi table first and insert data into the Hudi table using SQL ",(0,i.kt)("inlineCode",{parentName:"p"},"VALUES")," as below."),(0,i.kt)("pre",null,(0,i.kt)("code",{parentName:"pre",className:"language-sql"},"-- sets up the result mode to tableau to show the results directly in the CLI\nset sql-client.execution.result-mode = tableau;\n\nCREATE TABLE t1(\n  uuid VARCHAR(20) PRIMARY KEY NOT ENFORCED,\n  name VARCHAR(10),\n  age INT,\n  ts TIMESTAMP(3),\n  `partition` VARCHAR(20)\n)\nPARTITIONED BY (`partition`)\nWITH (\n  'connector' = 'hudi',\n  'path' = '${path}',\n  'table.type' = 'MERGE_ON_READ' -- this creates a MERGE_ON_READ table, by default is COPY_ON_WRITE\n);\n\n-- insert data using values\nINSERT INTO t1 VALUES\n  ('id1','Danny',23,TIMESTAMP '1970-01-01 00:00:01','par1'),\n  ('id2','Stephen',33,TIMESTAMP '1970-01-01 00:00:02','par1'),\n  ('id3','Julian',53,TIMESTAMP '1970-01-01 00:00:03','par2'),\n  ('id4','Fabian',31,TIMESTAMP '1970-01-01 00:00:04','par2'),\n  ('id5','Sophia',18,TIMESTAMP '1970-01-01 00:00:05','par3'),\n  ('id6','Emma',20,TIMESTAMP '1970-01-01 00:00:06','par3'),\n  ('id7','Bob',44,TIMESTAMP '1970-01-01 00:00:07','par4'),\n  ('id8','Han',56,TIMESTAMP '1970-01-01 00:00:08','par4');\n"))),(0,i.kt)(o.Z,{value:"dataStream",mdxType:"TabItem"},(0,i.kt)("p",null,"Creates a Flink Hudi table first and insert data into the Hudi table using DataStream API as below."),(0,i.kt)("pre",null,(0,i.kt)("code",{parentName:"pre",className:"language-java"},'import org.apache.flink.streaming.api.datastream.DataStream;\nimport org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;\nimport org.apache.flink.table.data.RowData;\nimport org.apache.hudi.common.model.HoodieTableType;\nimport org.apache.hudi.configuration.FlinkOptions;\nimport org.apache.hudi.util.HoodiePipeline;\n\nStreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();\nString targetTable = "t1";\nString basePath = "file:///tmp/t1";\n\nMap<String, String> options = new HashMap<>();\noptions.put(FlinkOptions.PATH.key(), basePath);\noptions.put(FlinkOptions.TABLE_TYPE.key(), HoodieTableType.MERGE_ON_READ.name());\noptions.put(FlinkOptions.PRECOMBINE_FIELD.key(), "ts");\n\nDataStream<RowData> dataStream = env.addSource(...);\nHoodiePipeline.Builder builder = HoodiePipeline.builder(targetTable)\n    .column("uuid VARCHAR(20)")\n    .column("name VARCHAR(10)")\n    .column("age INT")\n    .column("ts TIMESTAMP(3)")\n    .column("`partition` VARCHAR(20)")\n    .pk("uuid")\n    .partition("partition")\n    .options(options);\n\nbuilder.sink(dataStream, false); // The second parameter indicating whether the input data stream is bounded\nenv.execute("Api_Sink");\n')))),(0,i.kt)("h3",{id:"query-data"},"Query Data"),(0,i.kt)(r.Z,{defaultValue:"flinksql",values:[{label:"Flink SQL",value:"flinksql"},{label:"DataStream API",value:"dataStream"}],mdxType:"Tabs"},(0,i.kt)(o.Z,{value:"flinksql",mdxType:"TabItem"},(0,i.kt)("pre",null,(0,i.kt)("code",{parentName:"pre",className:"language-sql"},"-- query from the Hudi table\nselect * from t1;\n"))),(0,i.kt)(o.Z,{value:"dataStream",mdxType:"TabItem"},(0,i.kt)("pre",null,(0,i.kt)("code",{parentName:"pre",className:"language-java"},'import org.apache.flink.streaming.api.datastream.DataStream;\nimport org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;\nimport org.apache.flink.table.data.RowData;\nimport org.apache.hudi.common.model.HoodieTableType;\nimport org.apache.hudi.configuration.FlinkOptions;\nimport org.apache.hudi.util.HoodiePipeline;\n\nStreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();\nString targetTable = "t1";\nString basePath = "file:///tmp/t1";\n\nMap<String, String> options = new HashMap<>();\noptions.put(FlinkOptions.PATH.key(), basePath);\noptions.put(FlinkOptions.TABLE_TYPE.key(), HoodieTableType.MERGE_ON_READ.name());\noptions.put(FlinkOptions.READ_AS_STREAMING.key(), "true"); // this option enable the streaming read\noptions.put(FlinkOptions.READ_START_COMMIT.key(), "\'20210316134557\'"); // specifies the start commit instant time\n    \nHoodiePipeline.Builder builder = HoodiePipeline.builder(targetTable)\n    .column("uuid VARCHAR(20)")\n    .column("name VARCHAR(10)")\n    .column("age INT")\n    .column("ts TIMESTAMP(3)")\n    .column("`partition` VARCHAR(20)")\n    .pk("uuid")\n    .partition("partition")\n    .options(options);\n\nDataStream<RowData> rowDataDataStream = builder.source(env);\nrowDataDataStream.print();\nenv.execute("Api_Source");\n')))),(0,i.kt)("p",null,"This statement queries snapshot view of the dataset.\nRefers to ",(0,i.kt)("a",{parentName:"p",href:"/docs/concepts#table-types--queries"},"Table types and queries")," for more info on all table types and query types supported."),(0,i.kt)("h3",{id:"update-data"},"Update Data"),(0,i.kt)("p",null,"This is similar to inserting new data."),(0,i.kt)("pre",null,(0,i.kt)("code",{parentName:"pre",className:"language-sql"},"-- this would update the record with key 'id1'\ninsert into t1 values\n  ('id1','Danny',27,TIMESTAMP '1970-01-01 00:00:01','par1');\n")),(0,i.kt)("p",null,"Notice that the save mode is now ",(0,i.kt)("inlineCode",{parentName:"p"},"Append"),". In general, always use append mode unless you are trying to create the table for the first time.\n",(0,i.kt)("a",{parentName:"p",href:"#query-data"},"Querying")," the data again will now show updated records. Each write operation generates a new ",(0,i.kt)("a",{parentName:"p",href:"/docs/concepts"},"commit"),"\ndenoted by the timestamp. Look for changes in ",(0,i.kt)("inlineCode",{parentName:"p"},"_hoodie_commit_time"),", ",(0,i.kt)("inlineCode",{parentName:"p"},"age")," fields for the same ",(0,i.kt)("inlineCode",{parentName:"p"},"_hoodie_record_key"),"s in previous commit."),(0,i.kt)("h3",{id:"streaming-query"},"Streaming Query"),(0,i.kt)("p",null,"Hudi Flink also provides capability to obtain a stream of records that changed since given commit timestamp.\nThis can be achieved using Hudi's streaming querying and providing a start time from which changes need to be streamed.\nWe do not need to specify endTime, if we want all changes after the given commit (as is the common case). "),(0,i.kt)("div",{className:"admonition admonition-note alert alert--secondary"},(0,i.kt)("div",{parentName:"div",className:"admonition-heading"},(0,i.kt)("h5",{parentName:"div"},(0,i.kt)("span",{parentName:"h5",className:"admonition-icon"},(0,i.kt)("svg",{parentName:"span",xmlns:"http://www.w3.org/2000/svg",width:"14",height:"16",viewBox:"0 0 14 16"},(0,i.kt)("path",{parentName:"svg",fillRule:"evenodd",d:"M6.3 5.69a.942.942 0 0 1-.28-.7c0-.28.09-.52.28-.7.19-.18.42-.28.7-.28.28 0 .52.09.7.28.18.19.28.42.28.7 0 .28-.09.52-.28.7a1 1 0 0 1-.7.3c-.28 0-.52-.11-.7-.3zM8 7.99c-.02-.25-.11-.48-.31-.69-.2-.19-.42-.3-.69-.31H6c-.27.02-.48.13-.69.31-.2.2-.3.44-.31.69h1v3c.02.27.11.5.31.69.2.2.42.31.69.31h1c.27 0 .48-.11.69-.31.2-.19.3-.42.31-.69H8V7.98v.01zM7 2.3c-3.14 0-5.7 2.54-5.7 5.68 0 3.14 2.56 5.7 5.7 5.7s5.7-2.55 5.7-5.7c0-3.15-2.56-5.69-5.7-5.69v.01zM7 .98c3.86 0 7 3.14 7 7s-3.14 7-7 7-7-3.12-7-7 3.14-7 7-7z"}))),"note")),(0,i.kt)("div",{parentName:"div",className:"admonition-content"},(0,i.kt)("p",{parentName:"div"},"The bundle jar with ",(0,i.kt)("strong",{parentName:"p"},"hive profile")," is needed for streaming query, by default the officially released flink bundle is built ",(0,i.kt)("strong",{parentName:"p"},"without"),"\n",(0,i.kt)("strong",{parentName:"p"},"hive profile"),", the jar needs to be built manually, see ",(0,i.kt)("a",{parentName:"p",href:"/docs/syncing_metastore#install"},"Build Flink Bundle Jar")," for more details."))),(0,i.kt)("pre",null,(0,i.kt)("code",{parentName:"pre",className:"language-sql"},"CREATE TABLE t1(\n  uuid VARCHAR(20) PRIMARY KEY NOT ENFORCED,\n  name VARCHAR(10),\n  age INT,\n  ts TIMESTAMP(3),\n  `partition` VARCHAR(20)\n)\nPARTITIONED BY (`partition`)\nWITH (\n  'connector' = 'hudi',\n  'path' = '${path}',\n  'table.type' = 'MERGE_ON_READ',\n  'read.streaming.enabled' = 'true',  -- this option enable the streaming read\n  'read.start-commit' = '20210316134557', -- specifies the start commit instant time\n  'read.streaming.check-interval' = '4' -- specifies the check interval for finding new source commits, default 60s.\n);\n\n-- Then query the table in stream mode\nselect * from t1;\n")),(0,i.kt)("p",null,"This will give all changes that happened after the ",(0,i.kt)("inlineCode",{parentName:"p"},"read.start-commit")," commit. The unique thing about this\nfeature is that it now lets you author streaming pipelines on streaming or batch data source."),(0,i.kt)("h3",{id:"deletes"},"Delete Data"),(0,i.kt)("p",null,"When consuming data in streaming query, Hudi Flink source can also accepts the change logs from the underneath data source,\nit can then applies the UPDATE and DELETE by per-row level. You can then sync a NEAR-REAL-TIME snapshot on Hudi for all kinds\nof RDBMS."),(0,i.kt)("h2",{id:"where-to-go-from-here"},"Where To Go From Here?"),(0,i.kt)("p",null,"Check out the ",(0,i.kt)("a",{parentName:"p",href:"/docs/next/flink_configuration"},"Flink Setup")," how-to page for deeper dive into configuration settings. "),(0,i.kt)("p",null,"If you are relatively new to Apache Hudi, it is important to be familiar with a few core concepts:"),(0,i.kt)("ul",null,(0,i.kt)("li",{parentName:"ul"},(0,i.kt)("a",{parentName:"li",href:"/docs/next/timeline"},"Hudi Timeline")," \u2013 How Hudi manages transactions and other table services"),(0,i.kt)("li",{parentName:"ul"},(0,i.kt)("a",{parentName:"li",href:"/docs/next/file_layouts"},"Hudi File Layout")," - How the files are laid out on storage"),(0,i.kt)("li",{parentName:"ul"},(0,i.kt)("a",{parentName:"li",href:"/docs/next/table_types"},"Hudi Table Types")," \u2013 ",(0,i.kt)("inlineCode",{parentName:"li"},"COPY_ON_WRITE")," and ",(0,i.kt)("inlineCode",{parentName:"li"},"MERGE_ON_READ")),(0,i.kt)("li",{parentName:"ul"},(0,i.kt)("a",{parentName:"li",href:"/docs/next/table_types#query-types"},"Hudi Query Types")," \u2013 Snapshot Queries, Incremental Queries, Read-Optimized Queries")),(0,i.kt)("p",null,'See more in the "Concepts" section of the docs.'),(0,i.kt)("p",null,"Take a look at recent ",(0,i.kt)("a",{parentName:"p",href:"/blog"},"blog posts")," that go in depth on certain topics or use cases."),(0,i.kt)("p",null,"Hudi tables can be queried from query engines like Hive, Spark, Flink, Presto and much more. We have put together a\n",(0,i.kt)("a",{parentName:"p",href:"https://www.youtube.com/watch?v=VhNgUsxdrD0"},"demo video")," that show cases all of this on a docker based setup with all\ndependent systems running locally. We recommend you replicate the same setup and run the demo yourself, by following\nsteps ",(0,i.kt)("a",{parentName:"p",href:"/docs/docker_demo"},"here")," to get a taste for it. Also, if you are looking for ways to migrate your existing data\nto Hudi, refer to ",(0,i.kt)("a",{parentName:"p",href:"/docs/migration_guide"},"migration guide"),"."))}m.isMDXComponent=!0}}]);