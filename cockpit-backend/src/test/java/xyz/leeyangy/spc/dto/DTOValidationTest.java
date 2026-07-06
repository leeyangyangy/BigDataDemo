package xyz.leeyangy.spc.dto;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import javax.validation.ConstraintViolation;
import javax.validation.Validation;
import javax.validation.Validator;
import javax.validation.ValidatorFactory;
import java.math.BigDecimal;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

/**
 * DTO 校验单元测试。
 *
 * <p>重点验证 P0 修复: 控制器直接绑定实体改为 DTO + @Valid 后, 必填字段能被正确校验。
 */
@DisplayName("DTO @Valid 校验测试")
class DTOValidationTest {

    private static Validator validator;

    @BeforeAll
    static void setUpValidator() {
        ValidatorFactory factory = Validation.buildDefaultValidatorFactory();
        validator = factory.getValidator();
    }

    // ==================== BatchCreateDTO ====================

    @Test
    @DisplayName("BatchCreateDTO - batchCode 为空时校验失败")
    void batchCreateDTO_BlankCode_FailsValidation() {
        BatchCreateDTO dto = new BatchCreateDTO();
        dto.setBatchCode("");  // 空字符串

        Set<ConstraintViolation<BatchCreateDTO>> violations = validator.validate(dto);

        assertFalse(violations.isEmpty(), "batchCode 为空时应校验失败");
        assertTrue(violations.stream()
                .anyMatch(v -> v.getPropertyPath().toString().equals("batchCode")));
    }

    @Test
    @DisplayName("BatchCreateDTO - batchCode 为 null 时校验失败")
    void batchCreateDTO_NullCode_FailsValidation() {
        BatchCreateDTO dto = new BatchCreateDTO();
        // batchCode 不设置, 为 null

        Set<ConstraintViolation<BatchCreateDTO>> violations = validator.validate(dto);

        assertFalse(violations.isEmpty(), "batchCode 为 null 时应校验失败");
    }

    @Test
    @DisplayName("BatchCreateDTO - batchCode 有效时校验通过")
    void batchCreateDTO_ValidCode_PassesValidation() {
        BatchCreateDTO dto = new BatchCreateDTO();
        dto.setBatchCode("BATCH-2026-001");

        Set<ConstraintViolation<BatchCreateDTO>> violations = validator.validate(dto);

        assertTrue(violations.isEmpty(), "batchCode 有效时应校验通过");
    }

    @Test
    @DisplayName("BatchCreateDTO - 仅空格的 batchCode 校验失败 (@NotBlank)")
    void batchCreateDTO_SpacesOnlyCode_FailsValidation() {
        BatchCreateDTO dto = new BatchCreateDTO();
        dto.setBatchCode("   ");

        Set<ConstraintViolation<BatchCreateDTO>> violations = validator.validate(dto);

        assertFalse(violations.isEmpty(), "@NotBlank 应拒绝纯空格字符串");
    }

    // ==================== SpcDataUploadDTO ====================

    @Test
    @DisplayName("SpcDataUploadDTO - 所有必填字段为 null 时校验失败")
    void spcDataUploadDTO_AllNull_FailsValidation() {
        SpcDataUploadDTO dto = new SpcDataUploadDTO();

        Set<ConstraintViolation<SpcDataUploadDTO>> violations = validator.validate(dto);

        assertEquals(5, violations.size(), "productId/processId/paramId/equipmentId/measuredValue 均为 @NotNull");
    }

    @Test
    @DisplayName("SpcDataUploadDTO - 所有必填字段有值时校验通过")
    void spcDataUploadDTO_AllPresent_PassesValidation() {
        SpcDataUploadDTO dto = new SpcDataUploadDTO();
        dto.setProductId(1L);
        dto.setProcessId(2L);
        dto.setParamId(3L);
        dto.setEquipmentId(4L);
        dto.setMeasuredValue(new BigDecimal("10.05"));

        Set<ConstraintViolation<SpcDataUploadDTO>> violations = validator.validate(dto);

        assertTrue(violations.isEmpty(), "所有必填字段有值时应校验通过");
    }

    @Test
    @DisplayName("SpcDataUploadDTO - productId 为 null 时校验失败")
    void spcDataUploadDTO_NullProductId_FailsValidation() {
        SpcDataUploadDTO dto = new SpcDataUploadDTO();
        dto.setProcessId(2L);
        dto.setParamId(3L);
        dto.setEquipmentId(4L);
        dto.setMeasuredValue(new BigDecimal("10.05"));

        Set<ConstraintViolation<SpcDataUploadDTO>> violations = validator.validate(dto);

        assertEquals(1, violations.size());
        assertEquals("productId", violations.iterator().next().getPropertyPath().toString());
    }

    @Test
    @DisplayName("SpcDataUploadDTO - measuredValue 为 null 时校验失败")
    void spcDataUploadDTO_NullMeasuredValue_FailsValidation() {
        SpcDataUploadDTO dto = new SpcDataUploadDTO();
        dto.setProductId(1L);
        dto.setProcessId(2L);
        dto.setParamId(3L);
        dto.setEquipmentId(4L);
        // measuredValue 不设置

        Set<ConstraintViolation<SpcDataUploadDTO>> violations = validator.validate(dto);

        assertEquals(1, violations.size());
        assertEquals("measuredValue", violations.iterator().next().getPropertyPath().toString());
    }

    // ==================== WorkshopCreateDTO ====================

    @Test
    @DisplayName("WorkshopCreateDTO - workshopCode 和 workshopName 均为空时校验失败")
    void workshopCreateDTO_BothBlank_FailsValidation() {
        WorkshopCreateDTO dto = new WorkshopCreateDTO();

        Set<ConstraintViolation<WorkshopCreateDTO>> violations = validator.validate(dto);

        assertEquals(2, violations.size(), "workshopCode 和 workshopName 均为 @NotBlank");
    }

    @Test
    @DisplayName("WorkshopCreateDTO - 仅 workshopCode 有值时校验失败 (workshopName 仍为空)")
    void workshopCreateDTO_OnlyCode_FailsValidation() {
        WorkshopCreateDTO dto = new WorkshopCreateDTO();
        dto.setWorkshopCode("WS-001");

        Set<ConstraintViolation<WorkshopCreateDTO>> violations = validator.validate(dto);

        assertEquals(1, violations.size());
        assertEquals("workshopName", violations.iterator().next().getPropertyPath().toString());
    }

    @Test
    @DisplayName("WorkshopCreateDTO - workshopCode 和 workshopName 均有值时校验通过")
    void workshopCreateDTO_BothPresent_PassesValidation() {
        WorkshopCreateDTO dto = new WorkshopCreateDTO();
        dto.setWorkshopCode("WS-001");
        dto.setWorkshopName("封装车间");

        Set<ConstraintViolation<WorkshopCreateDTO>> violations = validator.validate(dto);

        assertTrue(violations.isEmpty(), "必填字段有值时应校验通过");
    }

    // ==================== BatchUpdateDTO ====================

    @Test
    @DisplayName("BatchUpdateDTO - 所有字段为 null 时校验通过 (更新场景字段全可选)")
    void batchUpdateDTO_AllNull_PassesValidation() {
        BatchUpdateDTO dto = new BatchUpdateDTO();

        Set<ConstraintViolation<BatchUpdateDTO>> violations = validator.validate(dto);

        assertTrue(violations.isEmpty(), "更新 DTO 字段全可选, 空对象应校验通过");
    }
}
