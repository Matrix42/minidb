# IN、NOT IN、EXISTS、NOT EXISTS 的优化原理(通用理论)

这是一篇面向零基础的讲解:数据库内部是如何优化 `IN` / `NOT IN` / `EXISTS` / `NOT EXISTS` 这四种常见子查询写法的。读完后你会理解:它们本质在问什么、为什么有的快有的慢、为什么 `NOT IN` 是个「坑」,以及优化器在背后做了什么。

## 一、先弄懂这四种写法在「问什么」

假设有两张表:

- `员工(员工id, 部门id, 姓名)`
- `部门(部门id, 部门名)`

### 1. EXISTS / NOT EXISTS:问「有没有」

```sql
-- 找出「至少有一个下属的部门」
SELECT * FROM 部门 d
WHERE EXISTS (SELECT 1 FROM 员工 e WHERE e.部门id = d.部门id);
```

`EXISTS` 只关心**子查询里有没有至少一行**。它**不返回子查询的值**,只返回真/假。`NOT EXISTS` 就是反过来,问「一行都没有吗」。

关键点:`EXISTS` 不比较任何具体值,所以**和 NULL 基本无交集**——它只看「是否存在一行」,那一行的列是什么、是不是 NULL,都不重要。

### 2. IN / NOT IN:问「值在不在集合里」

```sql
-- 找出「有员工的那些部门的部门id」
SELECT * FROM 部门 d
WHERE d.部门id IN (SELECT e.部门id FROM 员工 e);
```

`IN` 问的是:`d.部门id` 这个**值**是否出现在子查询返回的**集合**里。这里涉及**值相等比较**,而值可能为 NULL,于是 NULL 就变成一个麻烦(后文详谈)。

一句话区分:

> `EXISTS` 关心「行的存在」;`IN` 关心「值的相等」。这一差别决定了它们的优化难度和 NULL 行为完全不同。

## 二、SQL 的 NULL 和三值逻辑(理解 NOT IN 坑的前提)

SQL 里的 `NULL` 表示「未知」,这导致逻辑判断有三种结果,而不是两种:

- `TRUE`(真)
- `FALSE`(假)
- `UNKNOWN`(未知)

几个关键规则:

| 表达式 | 结果 |
|--------|------|
| `1 = 1` | TRUE |
| `1 = NULL` | **UNKNOWN**(不是 FALSE!) |
| `NULL = NULL` | **UNKNOWN**(不是 TRUE!) |
| `NULL IS NULL` | TRUE(判断 NULL 要用 `IS NULL`,不能用 `=`) |

`NULL` 和任何值(包括 NULL 自己)用 `=` 比较,结果都是 `UNKNOWN`。

而在 `WHERE` 子句里,**只有 TRUE 的行会被保留**;`FALSE` 和 `UNKNOWN` 的行都被丢弃。这个「UNKNOWN 被当成假来过滤」是理解 `NOT IN` 坑的关键。

## 三、核心概念:子查询展开与去相关

### 1. 朴素做法(慢):逐行执行子查询

一个最直观的执行方式是「相关执行」:外层每一行,都去把子查询跑一遍。

```sql
SELECT * FROM 员工 e
WHERE EXISTS (SELECT 1 FROM 部门 d WHERE d.部门id = e.部门id);
```

朴素做法:对 `员工` 表的**每一行**,都把内层 `SELECT` 执行一次。如果员工有 10 万行,子查询就要跑 10 万次。这叫**相关子查询(Correlated Subquery)**,内层引用了外层的列(`e.部门id`),性能通常很差。

### 2. 优化器的核心思路:子查询展开(Subquery Unnesting)

数据库优化器几乎都会做一件事:**把子查询「展开」成 join**。

上面的 `EXISTS` 例子,展开后等价于:

```sql
SELECT e.* FROM 员工 e
JOIN 部门 d ON e.部门id = d.部门id;  -- 概念上的等价
```

把「每行跑一次子查询」变成「一次 join」,就能用上哈希连接、索引、并行等成熟技术。

### 3. 非相关 vs 相关子查询

- **非相关子查询**:内层不引用外层列,可以**独立先算一次**。
  ```sql
  WHERE EXISTS (SELECT 1 FROM 部门 WHERE 部门id = 10)  -- 内层和外层无关
  ```
  这种最简单,优化器可以直接把子查询结果物化成一个集合/单行。

- **相关子查询**:内层引用了外层列(`e.部门id`),必须和外层一起处理。优化器的核心工作就是**去相关(Decorrelation)**——把「外层行 → 内层行」的引用关系,改写成 join 条件。

## 四、半连接(Semi Join)与反连接(Anti Join)

展开子查询时,优化器不会用普通的 INNER JOIN / LEFT JOIN,而是用两个专用逻辑算子:

### 1. 半连接(Semi Join)

`EXISTS` 和 `IN` 的语义本质是**半连接**:

> 返回左表中「至少能和右表匹配一次」的行,但**不返回右表的列**,且**每个左行最多返回一次**(即使右表有多个匹配)。

对比普通 INNER JOIN:如果右表有 3 个匹配,INNER JOIN 会返回 3 行,半连接只返回 1 行。

```sql
SELECT * FROM 部门 d
WHERE EXISTS (SELECT 1 FROM 员工 e WHERE e.部门id = d.部门id);
-- 部门「研发部」有 5 个员工,半连接只返回「研发部」1 行,不重复
```

**为什么用半连接而不是 INNER JOIN + DISTINCT?**
普通写法 `JOIN ... DISTINCT` 要先把所有匹配行连接出来(可能几十万行),再花时间**去重**。半连接在连接的同时就「只保留一次匹配」,省掉了去重这一步,更快。

### 2. 反连接(Anti Join)

`NOT EXISTS` 和 `NOT IN` 的语义是**反连接**:

> 返回左表中「和右表没有匹配」的行。

```sql
SELECT * FROM 部门 d
WHERE NOT EXISTS (SELECT 1 FROM 员工 e WHERE e.部门id = d.部门id);
-- 返回「一个员工都没有」的部门
```

反连接可以用「LEFT JOIN + 右表列为 NULL」来模拟(右表没匹配上的行,连接后右表列是 NULL),但真正的反连接算子在连接时就判断「有没有匹配」,通常更快、更省内存。

## 五、逐个看四种谓词怎么优化

### 1. EXISTS → 半连接

```sql
SELECT * FROM 部门 d
WHERE EXISTS (SELECT 1 FROM 员工 e WHERE e.部门id = d.部门id);
```

优化为:**`部门` 半连接 `员工`(连接条件 `e.部门id = d.部门id`)**。

因为 `EXISTS` 只问「有没有」,不比较值,所以:
- 不需要去重右表;
- 不需要处理 NULL(即使 `e.部门id` 是 NULL,`NULL = d.部门id` 是 UNKNOWN,自然不匹配,不影响「是否存在其它匹配」)。

### 2. NOT EXISTS → 反连接

```sql
WHERE NOT EXISTS (SELECT 1 FROM 员工 e WHERE e.部门id = d.部门id);
```

优化为:**`部门` 反连接 `员工`**。同样因为只问「有没有」,最干净。

### 3. IN → 半连接(但要先保证集合语义)

```sql
WHERE d.部门id IN (SELECT e.部门id FROM 员工 e);
```

优化为半连接,但有一个额外动作:**右表(子查询结果)要先按 `部门id` 去重**。

为什么要去重?因为 `IN` 是集合成员判断,`d.部门id` 在集合里「有」还是「没有」,和集合里有几个无关。右表去重后,半连接才符合「每个左行最多一次」的语义。

(实际上优化器把 `IN` 展开后,通常就是「半连接 + 右表去重」,去重可能通过哈希去重、或直接利用索引、或把子查询的 `GROUP BY` 下推实现。)

### 4. NOT IN → 反连接,但 NULL 让事情变得复杂

`NOT IN` 是最麻烦的。先看一个著名陷阱:

```sql
-- 假设 员工.部门id 里有 NULL
SELECT * FROM 部门 d
WHERE d.部门id NOT IN (SELECT e.部门id FROM 员工 e);
```

如果员工表里 `部门id` 有一个 NULL,那么 `d.部门id NOT IN (...)` 的判定过程是:

```
d.部门id = 1 → 1 不在集合? 要看集合
d.部门id = NULL → NULL 是否等于集合里的 NULL? 结果 UNKNOWN
```

三值逻辑的结论是:**只要子查询结果里出现过 NULL,整个 `NOT IN` 就永远不可能是 TRUE**(只会是 FALSE 或 UNKNOWN),而 WHERE 只保留 TRUE,所以**结果恒为空集**。

这是一个几乎所有初学者都踩过的坑:**`NOT IN` 遇到 NULL 就「静默地什么都不返回」**。

因此优化器要正确处理 `NOT IN`,必须**显式检测子查询里有没有 NULL**,计划会多出「计数」之类的步骤(比如统计子查询总行数和非 NULL 行数,两者不等就说明有 NULL)。这也是 `NOT IN` 通常比 `NOT EXISTS` 更慢、更难优化的原因。

## 六、EXISTS vs IN:谁快?怎么写?

### 语义上

| | 问的是 | 遇 NULL |
|---|--------|---------|
| `EXISTS` | 行是否存在 | 基本无影响 |
| `IN` | 值是否相等 | NULL 不匹配,但不至于「全空」 |

### 性能上

在成熟的数据库里,`EXISTS` 和 `IN` 经过优化后**通常性能相当**(都展开成半连接)。但有两个常见差异:

1. **`EXISTS` 更「省心」**:它不需要处理 NULL 语义,优化器的展开更直接。
2. **`IN` 子查询结果要先去重**:如果子查询结果很大且重复多,去重有开销。

### 经验法则

- 判断「是否存在匹配」,**优先用 `EXISTS`**;判断「值是否在某个集合里」,才用 `IN`。
- **避免 `NOT IN`**,尤其当子查询列可能为 NULL 时。要「不存在匹配」,用 `NOT EXISTS` 几乎总是更安全、更快:
  ```sql
  -- 不好:子查询有 NULL 时结果恒空
  WHERE 部门id NOT IN (SELECT 部门id FROM 员工);

  -- 好:不受 NULL 影响
  WHERE NOT EXISTS (SELECT 1 FROM 员工 e WHERE e.部门id = 部门.部门id);
  ```
  如果确实要用 `NOT IN`,得先确保子查询列 `IS NOT NULL`,或写成 `NOT IN (SELECT ... WHERE 列 IS NOT NULL)`。

## 七、相关子查询的去相关手法(通用)

相关子查询(内层引用外层列)是优化器最要下功夫的地方。通用思路是**把「外层列引用」提升为 join 条件**:

```sql
-- 相关:外层 e.部门id 被内层引用
SELECT * FROM 员工 e
WHERE EXISTS (SELECT 1 FROM 部门 d WHERE d.部门id = e.部门id);
```

去相关后变成:

```
员工 e  半连接  部门 d   ON  d.部门id = e.部门id
```

对于更复杂的相关子查询,通用手法包括:

1. **把相关谓词提成 join 条件**(最常见)。
2. **给子查询加 `GROUP BY 相关列` 去重**,保证集合/存在语义(每个外层行最多匹配一次)。
3. **对 `NOT IN` 额外做 NULL 检测**(计数法:比较总行数和非 NULL 行数)。
4. **标量子查询**(子查询返回单个值)通常展开成 **LEFT JOIN**,没匹配时补 NULL。

如果优化器**没能**去相关(比如遇到太复杂的子查询),就会退化为「逐行执行子查询」的相关执行,性能骤降——这就是「相关子查询慢」的由来。

## 八、一句话总结

| 谓词 | 本质 | 优化成 | NULL 影响 |
|------|------|--------|-----------|
| `EXISTS` | 存在半连接 | 半连接 | 无 |
| `NOT EXISTS` | 不存在反连接 | 反连接 | 无 |
| `IN` | 值成员判断 | 半连接 + 右表去重 | NULL 不匹配(可接受) |
| `NOT IN` | 值非成员判断 | 反连接 + NULL 检测 | **子查询含 NULL 则结果恒空** |

- `EXISTS` / `NOT EXISTS` 关心「行的有无」,优化干净、无 NULL 坑。
- `IN` / `NOT IN` 关心「值的相等」,要处理集合去重和 NULL 三值逻辑,其中 `NOT IN` 最复杂。
- 优化器的核心工作就是「子查询展开 + 去相关」,把它们统一成半连接/反连接,从而复用成熟的 join 优化(哈希、索引、并行)。
- 实际写 SQL 时,判断存在性优先 `EXISTS`/`NOT EXISTS`,能避开 `NOT IN` 的 NULL 陷阱。
