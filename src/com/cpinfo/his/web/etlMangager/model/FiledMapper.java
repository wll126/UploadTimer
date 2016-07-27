package com.cpinfo.his.web.etlMangager.model;

import java.lang.annotation.*;

@Target(ElementType.FIELD)
@Retention(RetentionPolicy.RUNTIME)
@Documented
@Inherited

public @interface FiledMapper {
	//public int id(); 
	public String filed();

}
