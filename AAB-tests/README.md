# AAB tests

Тесты применялись для заранее засплитованных пользователей, т.е. проводился анализ результатов.

### 1. AA_test_pvalue_distr.ipynb
Задача:
```
"Есть данные АА-теста с '2022-01-24' по '2023-01-30'. Необходимо сделать симуляцию, как будто проведено 10000 АА-тестов.

На каждой итерации нужно сформировать подвыборки без повторения в 500 юзеров из 2 и 3 экспериментальной группы.

Провести сравнение этих подвыборок t-testом."
```
Необходимо сделать:
- Построить гистограмму распределения получившихся 10000 p-values
- Посчитать, какой процент p values оказался меньше либо равен 0.05
- Написать вывод по проведенному АА-тесту, корректно ли работает наша система сплитования

### 2. AB_test.ipynb
Задача:
```
"Эксперимент проходил с 2023-01-31 по 2023-02-06 включительно. Для эксперимента были задействованы 2 и 1 группы.

В группе 2 был использован один из новых алгоритмов рекомендации постов, группа 1 использовалась в качестве контроля.

Основная гипотеза заключается в том, что новый алгоритм во 2-й группе приведет к увеличению CTR.

Ваша задача — проанализировать данные АB-теста."
```

Необходимо сделать:
- Сравните данные рассмотренными тестами (я взял t-test). А еще посмотрите на распределения глазами. Почему тесты сработали так как сработали?
- Опишите потенциальную ситуацию, когда такое изменение могло произойти
- Напишите рекомендацию, будем ли мы раскатывать новый алгоритм на всех новых пользователей или все-таки не стоит

### 3. AB_test_linearized.ipynb
Необходимо сделать:
```
Проанализируйте тест между группами 0 и 3 по метрике линеаризованных лайков.

Видно ли отличие?

Стало ли p-value меньше?
```

Как считалась новая метрика?
- Считаем общий CTR в контрольной группе $𝐶𝑇𝑅𝑐𝑜𝑛𝑡𝑟𝑜𝑙=𝑠𝑢𝑚(𝑙𝑖𝑘𝑒𝑠)/𝑠𝑢𝑚(𝑣𝑖𝑒𝑤𝑠)$
- Посчитаем в обеих группах поюзерную метрику $𝑙𝑖𝑛𝑒𝑎𝑟𝑖𝑧𝑒𝑑_𝑙𝑖𝑘𝑒𝑠=𝑙𝑖𝑘𝑒𝑠−𝐶𝑇𝑅𝑐𝑜𝑛𝑡𝑟𝑜𝑙∗𝑣𝑖𝑒𝑤𝑠$
- После чего сравниваем t-тестом отличия в группах по метрике $𝑙𝑖𝑛𝑒𝑎𝑟𝑖𝑧𝑒𝑑_𝑙𝑖𝑘𝑒𝑠$
