package study.querydsl.dto;

import com.querydsl.core.types.dsl.*;

import com.querydsl.core.types.ConstructorExpression;
import javax.annotation.processing.Generated;

/**
 * study.querydsl.dto.QUserDto is a Querydsl Projection type for UserDto
 */
@Generated("com.querydsl.codegen.DefaultProjectionSerializer")
public class QUserDto extends ConstructorExpression<UserDto> {

    private static final long serialVersionUID = -897980559L;

    public QUserDto(com.querydsl.core.types.Expression<String> name, com.querydsl.core.types.Expression<Integer> age) {
        super(UserDto.class, new Class<?>[]{String.class, int.class}, name, age);
    }

}

