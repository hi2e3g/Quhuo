2021年新财报数据相关-说明文档

- 概述
    项目为Code项目分支，目的在于使用code项目产生的数据展示公司财报，最终理想状态为培训业务部门自己编写修改算法和配置产生财报页面；
    关于code项目请参考code项目相关说明文档；
    数据源：code项目业务部门通过系统填报数据；

- 项目流程

    现阶段：
    数据源数据 -> 筛选工具按页面输入条件对数据进行筛选 -> 将筛选后的数据加载"模板"进行计算&格式化数据 -> 输出结果end；

    未来优化：
    数据源数据 -> 筛选工具按页面输入条件对数据进行筛选 -> 将筛选后的数据加载"模板"进行计算 -> 页面按照格式配置对输出数据格式化并进行渲染 -> 输出结果end；

        筛选工具工作流程:
        加载"finance_template.yml"配置文件 -> 按配置生成筛选下拉备选内容 & 选取对应计算"模板" -> 根据实际选取内容筛选数据 -> 将筛选后的数据加载至模板进行计算；

- 原始数据结构说明

    重点字段含义在此说明：具体详情请参见excel表格；
        - ac_code：科目编号，可以理解为各种手指类型动作的编号；如主营业务收入、差旅费、一线服务费、招聘人数等 对应的编号；
        - ac_name：科目名称，可以理解为各种手指类型动作的名称；如主营业务收入、差旅费、一线服务费、招聘人数等；
        - ac_rule_kind：数据类型，code or team；
        - book_month：数据所属月份，如 202101；
        - money：科目对应的数值；
        - industry_id：场景ID
        - code_team_id：team专属，与该team有关联的code的id；
        - org_bu：team专属，team所属的事业部编号；
        - org_bu_division：team专属，team所属的分部编号；
        - team_cate：team专属，team类型 S、F、O1、O2；

- "模板"说明

    模板目录结构：
    cookbook/
        ｜
        ｜- code/  # code维度-场景分类的模板
        ｜    |
        ｜    |--- clean_chengtu/
        ｜    |          ｜
        ｜    |          ｜--- code & city & region & supplier & project.yml  # 保洁—澄涂
        ｜    |
        ｜    |--- clean_lailai/
        ｜    |          ｜
        ｜    |          ｜--- code & city & region & supplier & project.yml  # 保洁—来来
        ｜    |
        ｜    |--- logistics/
        ｜    |          ｜
        ｜    |          ｜--- code & city & region & supplier & project.yml  # 外卖
        ｜    |
        ｜    |---    other/
        ｜    |          ｜
        ｜    |          ｜--- code & city & region & supplier & project.yml  # 其他
        ｜    |
        ｜    |--- travel_bicycle/
        ｜    |          ｜
        ｜    |          ｜--- code & city & region & supplier & project.yml  # 出行-单车
        ｜    |
        ｜    |--- travel_taxi/
        ｜               ｜
        ｜               ｜--- code & city & region & supplier & project.yml  # 出行-网约车
        ｜
        ｜
        ｜- summary/  # 汇总的模板
        ｜    |
        ｜    |--- code: company_code & group_code & org_code.yml  # 汇总-code部分
        ｜    |
        ｜    |--- team: company_team & group_team & org_business & org_division & org_team.yml  # 汇总-team部分
        ｜    |
        ｜    |--- ac.yml  # 汇总-全部项目
        ｜
        ｜
        ｜- team/ # team维度的模板
        ｜    |
        ｜    |--- business & division & team.yml  # team
        ｜
        ｜
        ｜- finance_template.yml  # 筛选和调用配置文件

    方法&函数说明：

        dataset_vis:  # XXX 这部分不用管 XXX

        dataset_pipeline:  # 我们编辑的部分

            # 只对此项目新添加的方法进行说明；

            - row_eval_by_index:  # index间计算 & 给科目根据index定位，对index间进行计算生成新的index；
                exp_str: |  # 计算指标；计算表达式输入，按照输入的表达式进行index间的计算；
                            {{QS-10101014}} = {{Q-1010101}} + {{Q-1010202}} + {{Q-1010203}} + {{Q-1010204}}
                            {{CAD-401}} = {{C-401}} / {{month_days}}
                            {{QAO-1010101}} = {{Q-1010101}} / {{C-401}}
                index_col: ac_code  # 指定index，指定后将使用该index内的数据帧的值进行索引定位计算，默认为ac_code;
                exclude_cols: [ac_name]
                target_rows:  # 给科目添加中文名列；根据index值定位行，针对选中index内的指定col数据帧进行赋值操作；
                    - index: QS-10101014  # index值；会根据此值定位index（行）
                      cols:
                        ac_name: 主营业务收入  # 赋值修改；被赋值的数据帧col名：赋的值；

            - row_pad_value_by_index:  # 补充参与计算的科目index（等号右侧），背景为：当出现不存在的科目参与计算计算方法会报错，为解决此问题所以需要对所有参与计算的科目进行pad；
                index_col: ac_code  # 定位添加的科目编号所在的col；
                pad_values:  # 添加科目编号并赋值默认值，科目编号：默认值；
                    Q-1010101: 0
                    Q-1010102: 0
                    Q-1010103: 0

            - sort_by_columns:  # 对col进行排序；在sort_list中的列会按sort_list中的顺序优先排序在左侧，不在sort_list中的会随机排列在已排序的列右侧；
                sort_list: [ ac_code, ac_name, 合计 ]

            - sort_by_index:  # 对index进行排序；在sort_list中的行会严格按sort_list中的顺序从上倒下排序，不在sort_list中的不会出现在排序后的df中；
                sort_list: [QS-10101014,Q-1010101,C-401]

            - row_int_format_by_index:  # 对index_rows中的index科目进行正数 format； "{:.0f}".format(v);
                  index_rows: [
                      C-401, C-402, C-415, C-416, C-403, C-404, C-405, C-406, C-407, C-408, C-409, C-410, C-411, C-412, C-413, C-414
                  ]

            - row_pct_format_by_index:  # 对index_rows中的index科目进行百分比 format； "{:.2f}%.format(v);
                  index_rows: [
                      IPQ-1010101, IPQ-1010102, IPQ-1010202406, IPQ-1010202, IPQ-1010203, IPQ-1010204, IPQ-102, IPQS-2017, IPQS-1
                  ]

            - row_money_format_by_index:  # 对index_rows中的index科目进行金额 format； "¥{:,.2f}".format(v / 100);
                  index_rows: [
                      Q-1010101, Q-1010102, Q-1010202, Q-1010203, Q-1010204, Q-102, Q-10201, Q-10202, Q-10203, Q-701, Q-20101,
                  ]

            - row_float_format_by_index:  # 对index_rows中的index科目进行浮点数 format； "{:.2f}".format(v);
                  index_rows: [
                      CAD-401, CAO-402, CAD-415, CAO-416, CAD-405, CAH-405, CAH-406
                  ]

            # 特殊说明；以下这部分不太容易看懂的代码的目的为实现
            #   1、将表格按科目作为index、按不同聚合主体为col、以money作为值进行计算的透视表格；
            #   2、给透视后的表格增加合计列；
            #   3、将所选日期范围的天数作为科目添加在透视好的表格科目中；
            # 的骚操作；并且差不多每个yaml前边都有，性能也不咋地，未来重点优化对象；建议通过编写一个定向实现方法的方式实现优化；
              - df_select:  # 增加限定对筛选器筛选后的数据进行二次限定；
                  - '[industry_id] == @v1 & [platform_id] in @v2'
                  - v1: bao_jie
                    v2: [chengtu]
              - set_meta_days_by_column:  # 通过src_column col中的月份值计算对应月份的天数，并给dest_column col赋值；
                  src_column: book_month
                  dest_column: month_days
              - push_dataset:  # 将预处理好的数据暂存备用；
                  key: mini
              - df_groupby:  # 使用科目编号ac_code对数据进行汇总，对money进行求和获取每个科目的合计数据；
                  by: [ac_code]
              - df_sum:
                  column: [money]
              - df_reset_index: [ ]
              - df_rename_columns:
                  - money: 合计
              - push_dataset:  # 将聚合处理好的数据暂存备用；
                  key: sum_mini
              - use_df:
                  key: mini
              - df_groupby:  # 按照该模板对应的聚合层级 和 月份，对month_days（月天数）取中间值；
                  by: [platform_name, project_name, supplier_name, region_name, city_name, book_month]
              - df_median:
                  column: month_days
              - df_reset_index: [ ]
              - df_groupby:  # 按照该模板对应的聚合层级，对month_days（月天数）取合计值；得到的就是各个聚合主体所对应的月天数；
                  by: [platform_name, project_name, supplier_name, region_name, city_name]
              - df_sum:
                  column: month_days
              - df_reset_index: []
              - add_cols:  # 添加一个ac_code（科目编号）列；赋值为month_days等下透视后拼接用，一遍使天数以科目的身份进入透视后的表中；
                  - ac_code: month_days
              - df_groupby:  # 给month_days增加合计列；用median方法，因为月天数数据的合计不应该使所有主体的天数合；
                  by: [ac_code]
              - df_median:
                  column: month_days
              - df_reset_index: []
              - df_rename_columns:
                  - month_days: 合计
              - stash_push_df: []
              - use_df:
                  key: sum_mini
              - stash_push_df: []
              - stash_concat_df:  # 将计算好的month_days的合计拼接到刚缓存的合计表中；
                  drop_stash: True
              - push_dataset:
                  key: sum_mini

              - use_df:
                  key: mini
              - df_groupby:  # 重新来一遍刚才计算month_days的骚操作；
                  by: [platform_name, project_name, supplier_name, region_name, city_name, book_month]
              - df_median:
                  column: month_days
              - df_reset_index: []
              - df_rename_columns:
                  - month_days: money
              - df_groupby:
                  by: [platform_name, project_name, supplier_name, region_name, city_name]
              - df_sum:
                  column: money
              - df_reset_index: []
              - add_cols:
                  - ac_code: month_days
              - stash_push_df: []
              - use_df:
                  key: mini
              - stash_push_df: []
              - stash_concat_df:  # 将计算好的month_days表拼接到总表后头；相当于给总表加了一个month_days的科目；
                  drop_stash: True
              - pivot_table:  # 透视；
                  values: money
                  index: [ac_code, ac_name]
                  columns: [platform_name, project_name, supplier_name, region_name, city_name]
                  aggfunc: sum
              - stash_push_df: []
              - use_df:
                  key: sum_mini
              - stash_push_df: []
              - stash_join_df:  # 将合计表join回来；
                  on: [ ac_code ]
                  how: right
                  drop_stash: true

        datasource:  # XXX 这部分也不用管 XXX
