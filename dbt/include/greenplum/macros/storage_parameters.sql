{% macro storage_parameters(appendoptimized, blocksize, orientation, compresstype, compresslevel) %}
    {% set params = [] %}
    
    {% if appendoptimized is not none %}
        {% do params.append("appendoptimized=" ~ appendoptimized) %}
    {% endif %}
    
    {% if appendoptimized %}
        {% if blocksize is not none %}
            {% do params.append("blocksize=" ~ blocksize) %}
        {% endif %}
        
        {% if compresstype is not none %}
            {% do params.append("compresstype='" ~ compresstype ~ "'") %}
        {% endif %}
        
        {% if compresslevel is not none %}
            {% do params.append("compresslevel=" ~ compresslevel) %}
        {% endif %}
        
        {% if orientation is not none %}
            {% do params.append("orientation='" ~ orientation ~ "'") %}
        {% endif %}
    {% endif %}
    
    {% if params %}
        with ({{ params|join(', ') }})
    {% endif %}
{% endmacro %}