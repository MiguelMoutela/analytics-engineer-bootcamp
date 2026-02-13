--@MIGUELMOUTELA.WORLD_INEQUALITY_REPORTS_STAGE
--drop table miguelmoutela.world_inequality_reports_metadata;
--drop table miguelmoutela.world_inequality_reports_layout_raw

-- raw extract
list @dataexpert_student.miguelmoutela.world_inequality_reports_stage;
create or replace table miguelmoutela.world_inequality_reports_layout_raw as
select *
from (
    with report as (
        select 
            split_part("name" , '/', 2) as filename,
            "md5" as file_checksum
        from table(result_scan(last_query_id()))
    )
    , parsed_doc as (
        select
            file_checksum,
            filename,
            snowflake.cortex.ai_parse_document(
                to_file('@miguelmoutela.world_inequality_reports_stage', filename),
                { 'mode': 'LAYOUT', 'page_split': true, 'extract_images': true }
            ) as parsed_content
        from report
    )
    select
        file_checksum,
        filename,
        page.value:index::integer as page_number,
        page.value:content::varchar as page_text,
        img.value:id::varchar as image_id,
        img.value:image_base64::varchar as image_base64,
        current_timestamp() as processed_at
    from parsed_doc,
        lateral flatten(input => parsed_content:pages) page,
        lateral flatten(input => page.value:images, outer => true) img
    where page.value:content is not null
);


-- text extract
with page_paragraphs as (
    select
        file_checksum,
        filename,
        page_number + 1 as page_number,
        image_id,
        t.index as paragraph_number,
        trim(t.value) as paragraph
    from world_inequality_reports_layout_raw,
         lateral split_to_table(page_text, '\n\n') t(line)
    where contains(paragraph, image_id) or image_id is null
)
, ref_images as (
    select *, 
        array_agg(image_id) over (
            partition by file_checksum, filename, page_number, paragraph_number order by paragraph_number
        ) as images_in_paragraph
    from page_paragraphs
), hinted_paragraphs as (
    select 
        p.file_checksum,
        p.filename,
        p.page_number,
        r.image_id,
        p.paragraph_number,
        p.paragraph,
        object_construct(
            'type', case
                    when startswith(p.paragraph, '#')
                        or startswith(p.paragraph, 'Source:')
                        or startswith(p.paragraph, 'Notes:')
                        or startswith(p.paragraph, '[^0]')
                            then p.paragraph
                        else null
                    end,
            'images', case 
                      when array_size(r.images_in_paragraph) > 0
                          then images_in_paragraph
                          else null
                      end
        ) AS value_hint
    from page_paragraphs p
    left join ref_images r on (
        p.file_checksum = r.file_checksum 
        and p.filename = r.filename
        and p.page_number = r.page_number
        and p.paragraph_number = r.paragraph_number
        and p.image_id = r.image_id
    )
), nullable_hints as (
    select 
        file_checksum,
        filename,
        page_number,
        image_id,
        paragraph_number,
        paragraph,
        case
            when value_hint = object_construct()
                then null
            else value_hint
        end as value_hint
    from hinted_paragraphs
)
select *,
    coalesce(
        last_value(value_hint ignore nulls) over (
            partition by file_checksum
            order by page_number, paragraph_number
            rows between unbounded preceding and current row
        ),
        object_construct() 
    ) AS value_hint_filled
from nullable_hints
where length(paragraph) > 50
order by file_checksum, page_number, paragraph_number;


