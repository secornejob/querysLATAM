/*	Author:				Sebastian Esteban Cornejo Berrios (LATAM) <sebastian.cornejo@latam.com>
  *	Description:	Procedure to update current data and record a history about green ligth process.
  *	Created_at: 	2022/08/06
  *	Updated_at:		----/--/--
  *	Changes:			
  */


  /* Declaracion de variables.
    * Desc:		Se declaran variables a utilizar en el log de procedimientos.
    * params:
    *   >> proc_name: nombre general del procedimiento.
    *   >> proc_type: tipo de accion a realizar dentro del procedimiento.
    *   >> proc_description: descripcion de corta de regla de negocio.
    *   >> proc_target_table: tabla que se esta alimnetando.
    *   >> proc_datetime_start: fecha y hora de inicio del procedimiento.
    *   >> query_datetime_start: fecha y hora de inicio de query dentro del procedimiento.
    *   >> proc_datetime_end: fecha de termino del procedimiento.
    *   >> proc_records: cantidad de registros actualizados.
    *   >> proc_error_message: mensajes de error.
    */
    declare proc_name string default 'Green Ligth Brasil';
    declare proc_type string;
    declare proc_description string;
    declare proc_target_table string;
    declare proc_datetime_start datetime default current_datetime("America/Santiago");
    declare query_datetime_start datetime default proc_datetime_start;
    declare proc_datetime_end datetime;
    declare proc_records int64;
    declare proc_error_message string;


  /* Inicio de procedimiento
    * Desc:		Inicio de aplicacion de logicas para la actualizacion diaria de indicadores greenligth.
    * param:	
    */
    begin

      /* Actualizacion informacion actual
        * Desc:		Extracion de informacion de planilla Actual con la que se gestionan los greenligth de cada dia.
        * param:  
        * -- ?? select * from `sp-ed-opssupplyanalytics-dev.supply_sap.green_ligth_actual_br`
        */

        -- actualizacion de variables para log
        set proc_type = 'truncate/insert';
        set proc_description = 'update actual table';
        set proc_target_table = 'green_ligth_actual_br';


        -- consulta de actualizacion
        truncate table `sp-ed-opssupplyanalytics-dev.supply_sap.green_ligth_actual_br`;
        insert into `sp-ed-opssupplyanalytics-dev.supply_sap.green_ligth_actual_br`

        select 
          reparo,
          cast(lote as numeric) lote,
          concatenado,
          fornecedor,
          cast(po as numeric) po,
          part_number,
          responsable,
          criticidad,
          irm,
          task_force,
          pago,
          status_kab,
          gestionado,
          prioridade,
          cast(inicio as date format 'dd/mm/yyyy') as inicio,
          cast(ultima_gestion as date format 'dd/mm/yyyy') as ultima_gestion,
          finalizado,
          setor_pendente,
          comprador,
          tipo_da_pendencia,
          pendencia_curta,
          status,
          devolutiva,
          status_lote,
          criticidade,
          cast(freight_forwarder_codigo as numeric) as freight_forwarder_codigo,
          freight_forwarder_nome,
          pais,
          cast(data_chegada as date format 'dd/mm/yyyy') as data_chegada,
          cast(tempo_lote as numeric) as tempo_lote,
          invoice,
          situacao_invoice,
          descricao,
          serial_number,
          cast(qtde as numeric) as qtde,
          um,
          cast(valor_da_linha as numeric) as valor_da_linha,
          moeda,
          cast(valor_unitario as numeric) valor_unitario,
          nec_li,
          num_li,
          cast(data_pedido_li as numeric) as data_pedido_li,
          dt_def_li,
          dry_ice,
          dgr,
          observacao,
          grupo_comprador,
          area_acargo,
          coordenador,
          gerente,
          diretoria,
          tipo,
          cast(qt_lotes as numeric) as qt_lotes,
          cast(query_datetime_start as date) as fecha_actualizacion
        from `sp-ed-opssupplyanalytics-dev.supply_sap.ss_green_ligth_actual`;


        -- actualizacion de variables para log
        set proc_records = (select count(*) from `sp-ed-opssupplyanalytics-dev.supply_sap.green_ligth_actual_br`);
        set proc_datetime_end = current_datetime("America/Santiago");


        -- inserta log
        insert into `sp-ed-opssupplyanalytics-dev.supply_sap.audit_proc_log` 

        select 
          proc_name,
          proc_type,
          proc_description,
          proc_target_table,
          query_datetime_start,
          proc_datetime_end,
          proc_records,
          proc_error_message;


      /* Elimina informacion historica del dia a insertar
        * Desc:		Para evitar duplicados se eliminan todos los registros que tengan fecha de actualizacion identica a la fecha que se esta actualizando.
        * param  cast(query_datetime_start as date) >> indica la fecha de los registros que se tienen que borrar.
        * -- ?? select * from `sp-ed-opssupplyanalytics-dev.supply_sap.green_ligth_history_br`
        */

        -- actualizacion de variables para log
        set proc_type = 'delete from';
        set proc_description = 'delete the same date from history';
        set proc_target_table = 'green_ligth_history_br';
        set proc_records = (
          select count(*) 
          from `sp-ed-opssupplyanalytics-dev.supply_sap.green_ligth_history_br` 
          where cast(created_at_dt as date) = cast(query_datetime_start as date)
        );


        -- !! consulta de eliminacion
        delete from `sp-ed-opssupplyanalytics-dev.supply_sap.green_ligth_history_br`
        where cast(created_at_dt as date) = cast(query_datetime_start as date);


        -- actualizacion de variables para log
        set proc_datetime_end = current_datetime("America/Santiago");


        -- inserta log
        insert into `sp-ed-opssupplyanalytics-dev.supply_sap.audit_proc_log` 

        select 
          proc_name,
          proc_type,
          proc_description,
          proc_target_table,
          query_datetime_start,
          proc_datetime_end,
          proc_records,
          proc_error_message;


        
      /* Inserta informacion historica
        * Desc:		Se guarda informacion historica en base a lo actualizado en planilla actual.
        * param:  
        * -- ?? select * from `sp-ed-opssupplyanalytics-dev.supply_sap.green_ligth_history_br`
        */

        -- actualizacion de variables para log
        set query_datetime_start =  current_datetime("America/Santiago");
        set proc_type = 'insert into';
        set proc_description = 'insert into history table';
        set proc_records = (select count(*) from `sp-ed-opssupplyanalytics-dev.supply_sap.green_ligth_actual_br`);


        -- consulta de actualizacion
        insert into `sp-ed-opssupplyanalytics-dev.supply_sap.green_ligth_history_br` 

        select *
        from `sp-ed-opssupplyanalytics-dev.supply_sap.green_ligth_actual_br`;

        -- actualizacion de variables para log
        set proc_datetime_end = current_datetime("America/Santiago");


        -- inserta log
        insert into `sp-ed-opssupplyanalytics-dev.supply_sap.audit_proc_log` 

        select 
          proc_name,
          proc_type,
          proc_description,
          proc_target_table,
          query_datetime_start,
          proc_datetime_end,
          proc_records,
          proc_error_message;


  /* Control de errores.
    * Desc:		Si hay algun error en todo el procedimiento de actualizacion, se ejecutara esta sentencia que capturara el error
    * param:
    * -- ?? 
    */
    exception when error then
      set log_error = (SELECT @@error.message);
    end;


  /* Fin de procedimiento.
    * Desc:		Al finalizar el procedimiento se ejecutara esta sentencia que guardara un log general de actualizacion.
    * param:	
    * -- ?? 
    */

    -- actualizacion de variables para log
    set proc_type = 'update/insert procedure';
    set proc_description = 'green ligth procedure';
    set proc_target_table = null;
    set proc_datetime_end = current_datetime("America/Santiago");


    -- inserta log
    insert into `sp-ed-opssupplyanalytics-dev.supply_sap.audit_proc_log` (
      proc_name,
      proc_type,
      proc_description,
      proc_datetime_start,
      proc_datetime_end,
      proc_error_message
    )

    select 
      proc_name,
      proc_type,
      proc_description,
      proc_datetime_start,
      proc_datetime_end,
      proc_error_message;
