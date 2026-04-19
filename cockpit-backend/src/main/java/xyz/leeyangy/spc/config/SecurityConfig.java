package xyz.leeyangy.spc.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.HttpMethod;
import org.springframework.security.authentication.AuthenticationManager;
import org.springframework.security.config.annotation.authentication.configuration.AuthenticationConfiguration;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.annotation.web.configuration.EnableWebSecurity;
import org.springframework.security.config.http.SessionCreationPolicy;
import org.springframework.security.crypto.bcrypt.BCryptPasswordEncoder;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.security.web.SecurityFilterChain;
import org.springframework.security.web.authentication.UsernamePasswordAuthenticationFilter;
import xyz.leeyangy.spc.common.R;

import javax.servlet.http.HttpServletResponse;
import java.util.Map;

@Configuration
@EnableWebSecurity
@RequiredArgsConstructor
public class SecurityConfig {

    private final JwtAuthFilter jwtAuthFilter;
    private final ObjectMapper objectMapper;

    @Bean
    public PasswordEncoder passwordEncoder() {
        return new BCryptPasswordEncoder();
    }

    @Bean
    public AuthenticationManager authenticationManager(AuthenticationConfiguration config) throws Exception {
        return config.getAuthenticationManager();
    }

    @Bean
    public SecurityFilterChain filterChain(HttpSecurity http) throws Exception {
        http
                .csrf().disable()
                .sessionManagement().sessionCreationPolicy(SessionCreationPolicy.STATELESS)
                .and()
                .authorizeRequests(auth -> auth
                        .antMatchers("/api/auth/**").permitAll()
                        .antMatchers("/actuator/**").permitAll()
                        .antMatchers(HttpMethod.GET, "/api/spc/chart/**").authenticated()
                        .antMatchers(HttpMethod.GET, "/api/spc/stat/**").authenticated()
                        .antMatchers(HttpMethod.GET, "/api/spc/data/**").authenticated()
                        .antMatchers(HttpMethod.GET, "/api/spc/param-version/**").authenticated()
                        .antMatchers(HttpMethod.GET, "/api/spc/product/**").authenticated()
                        .antMatchers(HttpMethod.GET, "/api/spc/process/**").authenticated()
                        .antMatchers(HttpMethod.GET, "/api/spc/param/**").authenticated()
                        .antMatchers(HttpMethod.GET, "/api/spc/batch/**").authenticated()
                        .antMatchers(HttpMethod.GET, "/api/spc/alert/**").authenticated()
                        .antMatchers(HttpMethod.POST, "/api/spc/data/upload").hasAnyRole("ADMIN", "ENGINEER", "OPERATOR")
                        .antMatchers(HttpMethod.POST, "/api/spc/data/batch-upload").hasAnyRole("ADMIN", "ENGINEER", "OPERATOR")
                        .antMatchers(HttpMethod.POST, "/api/spc/data/import").hasAnyRole("ADMIN", "ENGINEER")
                        .antMatchers(HttpMethod.GET, "/api/spc/data/export/report").hasAnyRole("ADMIN", "ENGINEER")
                        .antMatchers(HttpMethod.POST, "/api/spc/stat/calculate").hasAnyRole("ADMIN", "ENGINEER","VIEWER")
                        .antMatchers(HttpMethod.POST, "/api/spc/param-version/create").hasAnyRole("ADMIN", "ENGINEER")
                        .antMatchers(HttpMethod.POST, "/api/spc/batch").hasAnyRole("ADMIN", "ENGINEER")
                        .antMatchers(HttpMethod.POST, "/api/spc/product").hasRole("ADMIN")
                        .antMatchers("/api/admin/**").hasRole("ADMIN")
                        .antMatchers(HttpMethod.PUT, "/api/spc/alert/**/ack").authenticated()
                        .antMatchers(HttpMethod.PUT, "/api/spc/alert/**/resolve").hasAnyRole("ADMIN", "ENGINEER")
                        .anyRequest().authenticated()
                )
                .exceptionHandling(ex -> ex
                        .authenticationEntryPoint((request, response, authException) -> {
                            response.setStatus(HttpServletResponse.SC_UNAUTHORIZED);
                            response.setContentType("application/json;charset=UTF-8");
                            response.getWriter().write(
                                    objectMapper.writeValueAsString(R.fail("未登录或Token已过期，请先登录"))
                            );
                        })
                        .accessDeniedHandler((request, response, accessDeniedException) -> {
                            response.setStatus(HttpServletResponse.SC_FORBIDDEN);
                            response.setContentType("application/json;charset=UTF-8");
                            response.getWriter().write(
                                    objectMapper.writeValueAsString(R.fail("权限不足"))
                            );
                        })
                )
                .addFilterBefore(jwtAuthFilter, UsernamePasswordAuthenticationFilter.class);

        return http.build();
    }
}
